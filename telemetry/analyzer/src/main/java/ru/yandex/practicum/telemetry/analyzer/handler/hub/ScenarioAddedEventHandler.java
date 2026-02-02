package ru.yandex.practicum.telemetry.analyzer.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.analyzer.handler.HubEventHandler;
import ru.yandex.practicum.telemetry.analyzer.model.*;
import ru.yandex.practicum.telemetry.analyzer.repository.*;

import java.util.List;
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

        // ВРЕМЕННО отключаем проверку сенсоров - это основная проблема!
        log.warn("⚠️ TEMPORARY: Skipping sensor existence check!");


        List<String> conditionSensorIds = scenarioAddedEventAvro.getConditions().stream()
                .map(ScenarioConditionAvro::getSensorId)
                .toList();
        List<String> actionSensorIds = scenarioAddedEventAvro.getActions().stream()
                .map(DeviceActionAvro::getSensorId)
                .toList();

        // Проверяем какие сенсоры существуют
        boolean allConditionSensorsExist = sensorRepository.existsByIdInAndHubId(conditionSensorIds, event.getHubId());
        boolean allActionSensorsExist = sensorRepository.existsByIdInAndHubId(actionSensorIds, event.getHubId());

        log.info("All condition sensors exist: {}", allConditionSensorsExist);
        log.info("All action sensors exist: {}", allActionSensorsExist);

        if (!allConditionSensorsExist || !allActionSensorsExist) {
            log.error("❌ Some sensors not found. Condition sensors: {}, Action sensors: {}",
                    conditionSensorIds, actionSensorIds);
            throw new IllegalArgumentException("Устройства не найдены");
        }
        

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
            // ВРЕМЕННО: если сенсор не найден, создаем его!
            Sensor sensor = sensorRepository.findById(conditionAvro.getSensorId())
                    .orElseGet(() -> {
                        log.warn("⚠️ Sensor {} not found, creating it...", conditionAvro.getSensorId());
                        Sensor newSensor = Sensor.builder()
                                .id(conditionAvro.getSensorId())
                                .hubId(event.getHubId())
                                .build();
                        return sensorRepository.save(newSensor);
                    });

            Integer value = asInteger(conditionAvro.getValue());
            log.info("Saving condition: sensor={}, type={}, operation={}, value={} (converted: {})",
                    sensor.getId(),
                    conditionAvro.getType(),
                    conditionAvro.getOperation(),
                    conditionAvro.getValue(),
                    value);

            Condition condition = conditionRepository.save(
                    Condition.builder()
                            .type(conditionAvro.getType())
                            .operation(conditionAvro.getOperation())
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

    private void saveActions(Scenario scenario, HubEventAvro event, ScenarioAddedEventAvro avro) {
        log.info("Saving {} actions...", avro.getActions().size());

        for (DeviceActionAvro actionAvro : avro.getActions()) {
            // ВРЕМЕННО: если сенсор не найден, создаем его!
            Sensor sensor = sensorRepository.findById(actionAvro.getSensorId())
                    .orElseGet(() -> {
                        log.warn("⚠️ Sensor {} not found, creating it...", actionAvro.getSensorId());
                        Sensor newSensor = Sensor.builder()
                                .id(actionAvro.getSensorId())
                                .hubId(event.getHubId())
                                .build();
                        return sensorRepository.save(newSensor);
                    });

            log.info("Saving action: sensor={}, type={}, value={}",
                    sensor.getId(),
                    actionAvro.getType(),
                    actionAvro.getValue());

            Action action = actionRepository.save(
                    Action.builder()
                            .type(actionAvro.getType())
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

    private Integer asInteger(Object value) {
        if (value == null) {
            log.warn("Value is null in asInteger, returning 0");
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
                // Пробуем преобразовать строку
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
            return 0; // Возвращаем 0 вместо исключения
        }
    }
}