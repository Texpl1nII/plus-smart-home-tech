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

            // ПРЕОБРАЗУЕМ ТИП - УПРОЩЕННЫЙ ВАРИАНТ
            ConditionTypeAvro typeAvro = ConditionTypeAvro.valueOf(conditionAvro.getType().toString());
            ConditionOperationAvro operationAvro = ConditionOperationAvro.valueOf(conditionAvro.getOperation().toString());

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

    private Integer extractConditionValue(Object value) {
        if (value == null) {
            return 0;
        }

        if (value instanceof Boolean) {
            return (Boolean) value ? 1 : 0;
        }

        if (value instanceof Integer) {
            return (Integer) value;
        }

        if (value instanceof Long) {
            long longVal = (Long) value;
            // Если это 1L или 0L (boolean в виде Long)
            if (longVal == 1L || longVal == 0L) {
                return (int) longVal;
            }
            return (int) longVal;
        }

        if (value instanceof Number) {
            return ((Number) value).intValue();
        }

        String str = value.toString().toLowerCase().trim();
        if ("true".equals(str) || "1".equals(str)) {
            return 1;
        }
        if ("false".equals(str) || "0".equals(str)) {
            return 0;
        }

        try {
            return Integer.parseInt(str);
        } catch (NumberFormatException e) {
            return 0;
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

            // ПРОСТОЕ ПРЕОБРАЗОВАНИЕ
            ActionTypeAvro typeAvro = ActionTypeAvro.valueOf(actionAvro.getType().toString());

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
}