package ru.yandex.practicum.telemetry.analyzer.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.analyzer.handler.HubEventHandler;
import ru.yandex.practicum.telemetry.analyzer.model.*;
import ru.yandex.practicum.telemetry.analyzer.repository.*;

import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioAddedEventHandler implements HubEventHandler {

    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;
    private final ScenarioActionRepository scenarioActionRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;

    @Override
    public String getEventType() {
        return ScenarioAddedEventAvro.class.getSimpleName();
    }

    @Override
    public void handle(HubEventAvro event) {
        log.info("=== SCENARIO_ADDED EVENT START ===");
        log.info("Hub: {}", event.getHubId());

        ScenarioAddedEventAvro scenarioAddedEventAvro = (ScenarioAddedEventAvro) event.getPayload();
        log.info("Scenario name: {}, Conditions: {}, Actions: {}",
                scenarioAddedEventAvro.getName(),
                scenarioAddedEventAvro.getConditions().size(),
                scenarioAddedEventAvro.getActions().size());

        // Проверяем существование сценария
        Optional<Scenario> existingScenario = scenarioRepository.findByHubIdAndName(
                event.getHubId(), scenarioAddedEventAvro.getName());

        if (existingScenario.isPresent()) {
            log.info("Updating existing scenario: {}", scenarioAddedEventAvro.getName());
            Scenario prevScenario = existingScenario.get();
            scenarioActionRepository.deleteByScenario(prevScenario);
            scenarioConditionRepository.deleteByScenario(prevScenario);
            scenarioRepository.deleteByHubIdAndName(
                    prevScenario.getHubId(),
                    prevScenario.getName()
            );
        } else {
            log.info("Creating new scenario: {}", scenarioAddedEventAvro.getName());
        }

        Scenario scenarioToUpload = Scenario.builder()
                .name(scenarioAddedEventAvro.getName())
                .hubId(event.getHubId())
                .build();

        Scenario savedScenario = scenarioRepository.save(scenarioToUpload);
        log.info("✅ Scenario saved with ID: {}", savedScenario.getId());

        saveConditions(savedScenario, event, scenarioAddedEventAvro);
        saveActions(savedScenario, event, scenarioAddedEventAvro);

        log.info("=== SCENARIO_ADDED EVENT END ===");
    }

    private void saveConditions(Scenario scenario, HubEventAvro event, ScenarioAddedEventAvro avro) {
        log.info("Saving {} conditions...", avro.getConditions().size());

        for (ScenarioConditionAvro conditionAvro : avro.getConditions()) {
            // Ищем сенсор
            Sensor sensor = sensorRepository.findByIdAndHubId(
                            conditionAvro.getSensorId(), event.getHubId())
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Sensor not found: " + conditionAvro.getSensorId() +
                                    " for hub: " + event.getHubId()));

            Integer value = extractConditionValue(conditionAvro.getValue());

            // КРИТИЧНО: преобразуем тип если нужно
            ConditionTypeAvro typeAvro;
            ConditionOperationAvro operationAvro;

            try {
                // Пробуем напрямую - если это уже Avro enum
                typeAvro = (ConditionTypeAvro) conditionAvro.getType();
                operationAvro = (ConditionOperationAvro) conditionAvro.getOperation();
                log.debug("Direct cast to Avro enums successful");
            } catch (ClassCastException e) {
                // Если не получается, преобразуем через строку
                log.warn("ClassCastException, converting via string...");
                typeAvro = convertToConditionTypeAvro(conditionAvro.getType());
                operationAvro = convertToConditionOperationAvro(conditionAvro.getOperation());
            }

            log.info("Saving condition: sensor={}, type={}, operation={}, value={}",
                    sensor.getId(), typeAvro, operationAvro, value);

            Condition condition = conditionRepository.save(
                    Condition.builder()
                            .type(typeAvro)
                            .operation(operationAvro)
                            .value(value)
                            .build()
            );

            scenarioConditionRepository.save(
                    ScenarioCondition.builder()
                            .scenario(scenario)
                            .sensor(sensor)
                            .condition(condition)
                            .id(new ScenarioConditionId(
                                    scenario.getId(),
                                    sensor.getId(),
                                    condition.getId()
                            ))
                            .build()
            );

            log.info("✅ Condition saved with ID: {}", condition.getId());
        }
    }

    private ConditionTypeAvro convertToConditionTypeAvro(Object type) {
        String typeStr = type.toString();
        try {
            return ConditionTypeAvro.valueOf(typeStr);
        } catch (IllegalArgumentException e) {
            log.error("Cannot convert to ConditionTypeAvro: {}", typeStr);
            // Маппинг для protobuf -> avro
            return switch (typeStr) {
                case "MOTION" -> ConditionTypeAvro.MOTION;
                case "LUMINOSITY" -> ConditionTypeAvro.LUMINOSITY;
                case "SWITCH" -> ConditionTypeAvro.SWITCH;
                case "TEMPERATURE" -> ConditionTypeAvro.TEMPERATURE;
                case "CO2LEVEL" -> ConditionTypeAvro.CO2LEVEL;
                case "HUMIDITY" -> ConditionTypeAvro.HUMIDITY;
                default -> throw new IllegalArgumentException("Unknown type: " + typeStr);
            };
        }
    }

    private ConditionOperationAvro convertToConditionOperationAvro(Object operation) {
        String opStr = operation.toString();
        try {
            return ConditionOperationAvro.valueOf(opStr);
        } catch (IllegalArgumentException e) {
            log.error("Cannot convert to ConditionOperationAvro: {}", opStr);
            return switch (opStr) {
                case "EQUALS" -> ConditionOperationAvro.EQUALS;
                case "GREATER_THAN" -> ConditionOperationAvro.GREATER_THAN;
                case "LOWER_THAN" -> ConditionOperationAvro.LOWER_THAN;
                default -> throw new IllegalArgumentException("Unknown operation: " + opStr);
            };
        }
    }

    private void saveActions(Scenario scenario, HubEventAvro event, ScenarioAddedEventAvro avro) {
        log.info("Saving {} actions...", avro.getActions().size());

        for (DeviceActionAvro actionAvro : avro.getActions()) {
            // Ищем сенсор
            Sensor sensor = sensorRepository.findByIdAndHubId(
                            actionAvro.getSensorId(), event.getHubId())
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Sensor not found: " + actionAvro.getSensorId() +
                                    " for hub: " + event.getHubId()));

            ActionTypeAvro typeAvro;
            try {
                typeAvro = (ActionTypeAvro) actionAvro.getType();
            } catch (ClassCastException e) {
                typeAvro = convertToActionTypeAvro(actionAvro.getType());
            }

            log.info("Saving action: sensor={}, type={}, value={}",
                    sensor.getId(), typeAvro, actionAvro.getValue());

            Action action = actionRepository.save(
                    Action.builder()
                            .type(typeAvro)
                            .value(actionAvro.getValue())
                            .build()
            );

            scenarioActionRepository.save(
                    ScenarioAction.builder()
                            .scenario(scenario)
                            .sensor(sensor)
                            .action(action)
                            .id(new ScenarioActionId(
                                    scenario.getId(),
                                    sensor.getId(),
                                    action.getId()
                            ))
                            .build()
            );

            log.info("✅ Action saved with ID: {}", action.getId());
        }
    }

    private ActionTypeAvro convertToActionTypeAvro(Object type) {
        String typeStr = type.toString();
        try {
            return ActionTypeAvro.valueOf(typeStr);
        } catch (IllegalArgumentException e) {
            return switch (typeStr) {
                case "ACTIVATE" -> ActionTypeAvro.ACTIVATE;
                case "DEACTIVATE" -> ActionTypeAvro.DEACTIVATE;
                case "INVERSE" -> ActionTypeAvro.INVERSE;
                case "SET_VALUE" -> ActionTypeAvro.SET_VALUE;
                default -> throw new IllegalArgumentException("Unknown action type: " + typeStr);
            };
        }
    }

    private Integer extractConditionValue(Object value) {
        if (value == null) {
            return 0;
        }

        try {
            if (value instanceof Integer) {
                return (Integer) value;
            } else if (value instanceof Boolean) {
                return (Boolean) value ? 1 : 0;
            } else if (value instanceof Long) {
                return ((Long) value).intValue();
            } else if (value instanceof Number) {
                return ((Number) value).intValue();
            } else {
                String strVal = value.toString().toLowerCase();
                if (strVal.equals("true") || strVal.equals("1")) {
                    return 1;
                } else if (strVal.equals("false") || strVal.equals("0")) {
                    return 0;
                } else {
                    return Integer.parseInt(strVal);
                }
            }
        } catch (Exception e) {
            log.error("Cannot convert value to Integer: {} (type: {})",
                    value, value.getClass().getName(), e);
            return 0;
        }
    }
}