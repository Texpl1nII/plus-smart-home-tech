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

        // Логируем условия
        for (ScenarioConditionAvro condition : scenarioAddedEventAvro.getConditions()) {
            log.info("Condition: sensor={}, type={}, operation={}, value={} (type: {})",
                    condition.getSensorId(),
                    condition.getType(),
                    condition.getOperation(),
                    condition.getValue(),
                    condition.getValue() != null ? condition.getValue().getClass().getSimpleName() : "null");
        }

        // Логируем действия
        for (DeviceActionAvro action : scenarioAddedEventAvro.getActions()) {
            log.info("Action: sensor={}, type={}, value={}",
                    action.getSensorId(),
                    action.getType(),
                    action.getValue());
        }

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

        scenarioRepository.save(scenarioToUpload);
        log.info("Scenario saved with ID: {}", scenarioToUpload.getId());

        saveConditions(scenarioToUpload, event, scenarioAddedEventAvro);
        saveActions(scenarioToUpload, event, scenarioAddedEventAvro);

        log.info("=== SCENARIO_ADDED EVENT END ===");
    }

    private void saveConditions(Scenario scenario, HubEventAvro event, ScenarioAddedEventAvro avro) {
        log.info("Saving {} conditions...", avro.getConditions().size());

        for (ScenarioConditionAvro conditionAvro : avro.getConditions()) {
            Sensor sensor = sensorRepository.findById(conditionAvro.getSensorId())
                    .orElseThrow(() -> new IllegalArgumentException("Сенсор не найден: " + conditionAvro.getSensorId()));

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

            log.info("Condition saved with ID: {}", condition.getId());
        }
    }

    private void saveActions(Scenario scenario, HubEventAvro event, ScenarioAddedEventAvro avro) {
        log.info("Saving {} actions...", avro.getActions().size());

        for (DeviceActionAvro actionAvro : avro.getActions()) {
            Sensor sensor = sensorRepository.findById(actionAvro.getSensorId())
                    .orElseThrow(() -> new IllegalArgumentException("Сенсор не найден: " + actionAvro.getSensorId()));

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

            log.info("Action saved with ID: {}", action.getId());
        }
    }

    private Integer asInteger(Object value) {
        if (value == null) {
            log.warn("Value is null in asInteger, returning null");
            return null;  // или 0 в зависимости от логики
        }
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Boolean) {
            return ((Boolean) value) ? 1 : 0;
        } else if (value instanceof Long) {
            return ((Long) value).intValue();
        }
        log.error("Unsupported type in asInteger: {}", value.getClass());
        throw new IllegalArgumentException("Неподдерживаемый тип значения: " + value.getClass());
    }
}