package ru.yandex.practicum.telemetry.analyzer.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.analyzer.entity.*;
import ru.yandex.practicum.telemetry.analyzer.handler.HubEventHandler;
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
    @Transactional
    public void handle(HubEventAvro event) {
        ScenarioAddedEventAvro scenarioEvent = (ScenarioAddedEventAvro) event.getPayload();

        log.info("=== PROCESSING SCENARIO ADDED EVENT ===");
        log.info("Hub: {}, Scenario: {}", event.getHubId(), scenarioEvent.getName());
        log.info("Conditions: {}, Actions: {}",
                scenarioEvent.getConditions().size(),
                scenarioEvent.getActions().size());

        // Проверяем существование сценария
        Optional<Scenario> existingScenario = scenarioRepository
                .findByHubIdAndName(event.getHubId(), scenarioEvent.getName());

        // Удаляем старый сценарий если существует
        existingScenario.ifPresent(scenario -> {
            log.info("Deleting existing scenario: {} (ID: {})",
                    scenario.getName(), scenario.getId());

            scenarioActionRepository.deleteByScenario(scenario);
            scenarioConditionRepository.deleteByScenario(scenario);
            scenarioRepository.delete(scenario);

            log.info("✓ Existing scenario deleted");
        });

        // Создаем новый сценарий
        Scenario scenario = Scenario.builder()
                .hubId(event.getHubId())
                .name(scenarioEvent.getName())
                .build();

        scenario = scenarioRepository.save(scenario);
        log.info("✓ New scenario created with ID: {}", scenario.getId());

        // Сохраняем условия
        log.info("Saving conditions...");
        int conditionIndex = 0;
        for (ScenarioConditionAvro conditionAvro : scenarioEvent.getConditions()) {
            try {
                log.info("  Condition {}: sensor={}, type={}, operation={}",
                        ++conditionIndex,
                        conditionAvro.getSensorId(),
                        conditionAvro.getType(),
                        conditionAvro.getOperation());

                saveCondition(scenario, conditionAvro);
                log.info("  ✓ Condition saved");
            } catch (Exception e) {
                log.error("❌ Failed to save condition {}: {}", conditionIndex, e.getMessage(), e);
                throw new RuntimeException("Failed to save condition", e);
            }
        }

        // Сохраняем действия
        log.info("Saving actions...");
        int actionIndex = 0;
        for (DeviceActionAvro actionAvro : scenarioEvent.getActions()) {
            try {
                log.info("  Action {}: sensor={}, type={}, value={}",
                        ++actionIndex,
                        actionAvro.getSensorId(),
                        actionAvro.getType(),
                        actionAvro.getValue());

                saveAction(scenario, actionAvro);
                log.info("  ✓ Action saved");
            } catch (Exception e) {
                log.error("❌ Failed to save action {}: {}", actionIndex, e.getMessage(), e);
                throw new RuntimeException("Failed to save action", e);
            }
        }

        log.info("=== SCENARIO SUCCESSFULLY SAVED ===");
        log.info("Scenario '{}' saved with {} conditions and {} actions",
                scenarioEvent.getName(),
                scenarioEvent.getConditions().size(),
                scenarioEvent.getActions().size());
    }

    private void saveCondition(Scenario scenario, ScenarioConditionAvro conditionAvro) {
        // Проверяем существование сенсора
        String sensorId = conditionAvro.getSensorId();
        String hubId = scenario.getHubId();

        log.debug("Looking for sensor: {} in hub: {}", sensorId, hubId);

        Sensor sensor = sensorRepository.findByIdAndHubId(sensorId, hubId)
                .orElseThrow(() -> {
                    String error = String.format("Sensor not found: %s in hub: %s", sensorId, hubId);
                    log.error(error);
                    return new IllegalArgumentException(error);
                });

        log.debug("Found sensor: {}", sensor.getId());

        // Извлекаем значение условия с проверкой
        Object conditionValue = conditionAvro.getValue();
        Integer intValue = extractConditionValue(conditionAvro.getType(), conditionValue);

        if (intValue == null) {
            String error = String.format("Cannot extract condition value for sensor %s, type %s, value %s (type: %s)",
                    sensorId, conditionAvro.getType(), conditionValue,
                    conditionValue != null ? conditionValue.getClass().getName() : "null");
            log.error(error);
            throw new IllegalArgumentException(error);
        }

        log.debug("Condition value extracted: {} -> {}", conditionValue, intValue);

        // Создаем условие
        Condition condition = Condition.builder()
                .type(conditionAvro.getType())
                .operation(conditionAvro.getOperation())
                .value(intValue)
                .build();

        condition = conditionRepository.save(condition);
        log.debug("Condition saved with ID: {}", condition.getId());

        // Создаем связь сценария с условием
        ScenarioCondition scenarioCondition = ScenarioCondition.builder()
                .id(new ScenarioConditionId(scenario.getId(), sensor.getId(), condition.getId()))
                .scenario(scenario)
                .sensor(sensor)
                .condition(condition)
                .build();

        scenarioConditionRepository.save(scenarioCondition);
        log.debug("ScenarioCondition saved");
    }

    private void saveAction(Scenario scenario, DeviceActionAvro actionAvro) {
        // Проверяем существование сенсора
        String sensorId = actionAvro.getSensorId();
        String hubId = scenario.getHubId();

        log.debug("Looking for sensor: {} in hub: {}", sensorId, hubId);

        Sensor sensor = sensorRepository.findByIdAndHubId(sensorId, hubId)
                .orElseThrow(() -> {
                    String error = String.format("Sensor not found: %s in hub: %s", sensorId, hubId);
                    log.error(error);
                    return new IllegalArgumentException(error);
                });

        log.debug("Found sensor: {}", sensor.getId());

        // Создаем действие
        Action action = Action.builder()
                .type(actionAvro.getType())
                .value(actionAvro.getValue())  // Внимание: value может быть null для некоторых типов действий!
                .build();

        action = actionRepository.save(action);
        log.debug("Action saved with ID: {}, type: {}, value: {}",
                action.getId(), action.getType(), action.getValue());

        // Создаем связь сценария с действием
        ScenarioAction scenarioAction = ScenarioAction.builder()
                .id(new ScenarioActionId(scenario.getId(), sensor.getId(), action.getId()))
                .scenario(scenario)
                .sensor(sensor)
                .action(action)
                .build();

        scenarioActionRepository.save(scenarioAction);
        log.debug("ScenarioAction saved");
    }

    private Integer extractConditionValue(ConditionTypeAvro conditionType, Object value) {
        if (value == null) {
            log.warn("Condition value is null for type: {}", conditionType);
            return null;
        }

        log.debug("Extracting value for type {}: value={} (class: {})",
                conditionType, value, value.getClass().getName());

        try {
            // В зависимости от типа условия, значение может быть boolean или integer
            return switch (conditionType) {
                case MOTION, SWITCH -> {
                    // Для датчиков движения и выключателей значение должно быть boolean
                    if (value instanceof Boolean) {
                        yield ((Boolean) value) ? 1 : 0;
                    } else if (value instanceof Integer) {
                        yield (Integer) value;
                    } else if (value instanceof Long) {
                        yield ((Long) value).intValue();
                    } else {
                        log.error("Unsupported value type for {}: {}", conditionType, value.getClass());
                        yield null;
                    }
                }
                case LUMINOSITY, TEMPERATURE, CO2LEVEL, HUMIDITY -> {
                    // Для остальных типов значение должно быть integer
                    if (value instanceof Integer) {
                        yield (Integer) value;
                    } else if (value instanceof Long) {
                        yield ((Long) value).intValue();
                    } else if (value instanceof Boolean) {
                        yield ((Boolean) value) ? 1 : 0;
                    } else {
                        log.error("Unsupported value type for {}: {}", conditionType, value.getClass());
                        yield null;
                    }
                }
                default -> {
                    log.error("Unknown condition type: {}", conditionType);
                    yield null;
                }
            };
        } catch (Exception e) {
            log.error("Error extracting condition value for type {}: {}", conditionType, e.getMessage(), e);
            return null;
        }
    }
}