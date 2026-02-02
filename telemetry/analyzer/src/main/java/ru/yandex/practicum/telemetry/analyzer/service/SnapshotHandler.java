package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.analyzer.grpc.HubRouterClient;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioAction;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioCondition;
import ru.yandex.practicum.telemetry.analyzer.repository.*;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotHandler {

    private final ScenarioRepository scenarioRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;
    private final HubRouterClient hubRouterClient;

    @Transactional(readOnly = true)
    public void handle(SensorsSnapshotAvro snapshot) {
        Map<String, SensorStateAvro> sensorStates = snapshot.getSensorsState();
        List<Scenario> scenarios = scenarioRepository.findByHubId(snapshot.getHubId());

        log.info("=== PROCESSING SNAPSHOT ===");
        log.info("Hub: {}, Sensors in snapshot: {}", snapshot.getHubId(), sensorStates.keySet());
        log.info("Found {} scenarios for hub {}", scenarios.size(), snapshot.getHubId());

        if (scenarios.isEmpty()) {
            log.warn("No scenarios found for hub: {}", snapshot.getHubId());
            return;
        }

        // Логируем все сценарии
        for (Scenario scenario : scenarios) {
            log.info("Checking scenario: {} (ID: {})", scenario.getName(), scenario.getId());

            List<ScenarioCondition> conditions = scenarioConditionRepository.findByScenario(scenario);
            List<ScenarioAction> actions = scenarioActionRepository.findByScenario(scenario);

            log.info("  Conditions: {}, Actions: {}", conditions.size(), actions.size());

            boolean conditionsMet = checkScenarioConditions(scenario, sensorStates);
            log.info("  Conditions met: {}", conditionsMet);

            if (conditionsMet) {
                log.info("  ✓ Executing actions for scenario: {}", scenario.getName());
                executeScenarioActions(scenario);
            }
        }

        log.info("=== SNAPSHOT PROCESSED ===");
    }

    private boolean checkScenarioConditions(Scenario scenario,
                                            Map<String, SensorStateAvro> sensorStates) {
        List<ScenarioCondition> conditions =
                scenarioConditionRepository.findByScenario(scenario);

        log.debug("Checking {} conditions for scenario: {}",
                conditions.size(), scenario.getName());

        return conditions.stream()
                .allMatch(condition -> {
                    boolean result = checkCondition(condition, sensorStates);
                    log.debug("  Condition {}: {}", condition.getId(), result ? "PASS" : "FAIL");
                    return result;
                });
    }

    private boolean checkCondition(ScenarioCondition scenarioCondition,
                                   Map<String, SensorStateAvro> sensorStates) {
        Condition condition = scenarioCondition.getCondition();
        String sensorId = scenarioCondition.getSensor().getId();
        SensorStateAvro sensorState = sensorStates.get(sensorId);

        if (sensorState == null) {
            log.warn("Sensor {} not found in snapshot for hub {}",
                    sensorId, scenarioCondition.getScenario().getHubId());
            return false;
        }

        Integer currentValue = extractSensorValue(condition.getType(), sensorState);
        if (currentValue == null) {
            log.warn("Could not extract value for sensor {} of type {}",
                    sensorId, condition.getType());
            return false;
        }

        boolean result = checkConditionOperation(condition.getOperation(),
                currentValue, condition.getValue());

        log.debug("  Sensor {}: current={}, target={}, operation={}, result={}",
                sensorId, currentValue, condition.getValue(),
                condition.getOperation(), result);

        return result;
    }

    private Integer extractSensorValue(ConditionTypeAvro conditionType,
                                       SensorStateAvro sensorState) {
        Object data = sensorState.getData();

        log.debug("Extracting value for type {}: data class = {}",
                conditionType, data != null ? data.getClass().getSimpleName() : "null");

        return switch (conditionType) {
            case MOTION -> {
                if (data instanceof MotionSensorAvro motion) {
                    yield motion.getMotion() ? 1 : 0;
                }
                yield null;
            }
            case LUMINOSITY -> {
                if (data instanceof LightSensorAvro light) {
                    yield light.getLuminosity();
                }
                yield null;
            }
            case SWITCH -> {
                if (data instanceof SwitchSensorAvro sw) {
                    yield sw.getState() ? 1 : 0;
                }
                yield null;
            }
            case TEMPERATURE -> {
                if (data instanceof ClimateSensorAvro climate) {
                    yield climate.getTemperatureC();
                }
                yield null;
            }
            case CO2LEVEL -> {
                if (data instanceof ClimateSensorAvro climate) {
                    yield climate.getCo2Level();
                }
                yield null;
            }
            case HUMIDITY -> {
                if (data instanceof ClimateSensorAvro climate) {
                    yield climate.getHumidity();
                }
                yield null;
            }
            default -> {
                log.error("Unknown condition type: {}", conditionType);
                yield null;
            }
        };
    }

    private boolean checkConditionOperation(ConditionOperationAvro operation,
                                            Integer currentValue, Integer targetValue) {
        return switch (operation) {
            case EQUALS -> currentValue.equals(targetValue);
            case GREATER_THAN -> currentValue > targetValue;
            case LOWER_THAN -> currentValue < targetValue;
        };
    }

    private void executeScenarioActions(Scenario scenario) {
        List<ScenarioAction> actions = scenarioActionRepository.findByScenario(scenario);

        log.info("Executing {} actions for scenario: {}",
                actions.size(), scenario.getName());

        actions.forEach(scenarioAction -> {
            try {
                log.info("  Sending action to sensor: {}, type: {}, value: {}",
                        scenarioAction.getSensor().getId(),
                        scenarioAction.getAction().getType(),
                        scenarioAction.getAction().getValue());

                hubRouterClient.sendDeviceRequest(scenarioAction);

                log.info("  ✓ Action sent successfully");
            } catch (Exception e) {
                log.error("Failed to execute action for scenario: {} on sensor: {}",
                        scenario.getName(), scenarioAction.getSensor().getId(), e);
            }
        });
    }
}
