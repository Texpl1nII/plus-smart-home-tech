package ru.yandex.practicum.telemetry.analyzer.handler.hub.snapshot;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.analyzer.service.HubRouterClient;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioCondition;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioActionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;

import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotHandler {

    private final ScenarioRepository scenarioRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;
    private final HubRouterClient hubRouterClient;

    @Transactional(readOnly = true)
    public void handle(SensorsSnapshotAvro sensorsSnapshotAvro) {
        log.info("=== DEBUG SNAPSHOT HANDLER START ===");

        // Детальное логирование ВСЕХ данных
        log.info("📸 Snapshot received: hub={}, sensorsCount={}",
                sensorsSnapshotAvro.getHubId(),
                sensorsSnapshotAvro.getSensorsState().size());

        // Логируем КАЖДЫЙ сенсор
        sensorsSnapshotAvro.getSensorsState().forEach((sensorId, state) -> {
            Object data = state.getData();
            log.info("Sensor {}: timestamp={}, dataType={}, data={}",
                    sensorId,
                    state.getTimestamp(),
                    data.getClass().getSimpleName(),
                    data.toString());
        });

        Map<String, SensorStateAvro> sensorStateMap = sensorsSnapshotAvro.getSensorsState();
        List<Scenario> scenariosList = scenarioRepository.findByHubId(sensorsSnapshotAvro.getHubId());

        log.info("Found {} scenarios for hub {}", scenariosList.size(), sensorsSnapshotAvro.getHubId());

        for (Scenario scenario : scenariosList) {
            log.info("🔍 Checking scenario: {} (hub: {})", scenario.getName(), scenario.getHubId());
            boolean shouldExecute = checkAllConditions(scenario, sensorStateMap);
            if (shouldExecute) {
                log.info("✅✅✅ ALL CONDITIONS MET! Executing scenario: {}", scenario.getName());
                executeScenarioActions(scenario);
            } else {
                log.info("❌ Some conditions not met for scenario: {}", scenario.getName());
            }
        }

        log.info("=== DEBUG SNAPSHOT HANDLER END ===");
    }

    private boolean checkAllConditions(Scenario scenario, Map<String, SensorStateAvro> sensorStateMap) {
        List<ScenarioCondition> conditions = scenarioConditionRepository.findByScenario(scenario);

        for (ScenarioCondition sc : conditions) {
            if (!checkSingleCondition(sc.getCondition(), sc.getSensor().getId(), sensorStateMap)) {
                log.debug("❌ Condition not met for scenario {}: sensor {}",
                        scenario.getName(), sc.getSensor().getId());
                return false;
            }
        }
        return true;
    }

    private boolean checkSingleCondition(Condition condition, String sensorId,
                                         Map<String, SensorStateAvro> sensorStateMap) {
        log.info("Checking condition: sensor={}, type={}, operation={}, value={}",
                sensorId, condition.getType(), condition.getOperation(), condition.getValue());

        SensorStateAvro sensorState = sensorStateMap.get(sensorId);
        if (sensorState == null) {
            log.warn("❌ Sensor {} not found in snapshot", sensorId);
            return false;
        }

        Integer currentValue = extractValue(sensorState, condition.getType());
        if (currentValue == null) {
            log.warn("❌ Could not extract value for sensor {} with type {}",
                    sensorId, condition.getType());
            return false;
        }

        boolean result = evaluateCondition(condition, currentValue);
        log.info("Condition result: {} (current={}, target={})",
                result ? "PASS" : "FAIL", currentValue, condition.getValue());

        return result;
    }

    private Integer extractValue(SensorStateAvro sensorState, ConditionTypeAvro type) {
        Object data = sensorState.getData();

        try {
            return switch (type) {
                case MOTION -> {
                    if (data instanceof MotionSensorAvro) {
                        MotionSensorAvro motion = (MotionSensorAvro) data;
                        yield motion.getMotion() ? 1 : 0;
                    }
                    log.error("Type mismatch for MOTION: expected MotionSensorAvro, got {}",
                            data.getClass().getSimpleName());
                    yield null;
                }
                case LUMINOSITY -> {
                    if (data instanceof LightSensorAvro) {
                        LightSensorAvro light = (LightSensorAvro) data;
                        yield light.getLuminosity();
                    }
                    log.error("Type mismatch for LUMINOSITY: expected LightSensorAvro, got {}",
                            data.getClass().getSimpleName());
                    yield null;
                }
                case SWITCH -> {
                    if (data instanceof SwitchSensorAvro) {
                        SwitchSensorAvro sw = (SwitchSensorAvro) data;
                        yield sw.getState() ? 1 : 0;
                    }
                    log.error("Type mismatch for SWITCH: expected SwitchSensorAvro, got {}",
                            data.getClass().getSimpleName());
                    yield null;
                }
                case TEMPERATURE -> {
                    // ← ВАЖНО: Может быть ДВА типа данных!
                    if (data instanceof ClimateSensorAvro) {
                        ClimateSensorAvro climate = (ClimateSensorAvro) data;
                        yield climate.getTemperatureC();
                    } else if (data instanceof TemperatureSensorAvro) {
                        TemperatureSensorAvro temp = (TemperatureSensorAvro) data;
                        yield temp.getTemperatureC();
                    } else {
                        log.error("Type mismatch for TEMPERATURE: expected ClimateSensorAvro or TemperatureSensorAvro, got {}",
                                data.getClass().getSimpleName());
                        yield null;
                    }
                }
                case CO2LEVEL -> {
                    if (data instanceof ClimateSensorAvro) {
                        ClimateSensorAvro climate = (ClimateSensorAvro) data;
                        yield climate.getCo2Level();
                    }
                    log.error("Type mismatch for CO2LEVEL: expected ClimateSensorAvro, got {}",
                            data.getClass().getSimpleName());
                    yield null;
                }
                case HUMIDITY -> {
                    if (data instanceof ClimateSensorAvro) {
                        ClimateSensorAvro climate = (ClimateSensorAvro) data;
                        yield climate.getHumidity();
                    }
                    log.error("Type mismatch for HUMIDITY: expected ClimateSensorAvro, got {}",
                            data.getClass().getSimpleName());
                    yield null;
                }
            };
        } catch (Exception e) {
            log.error("Error extracting value for type {}: {}", type, e.getMessage(), e);
            return null;
        }
    }

    private boolean evaluateCondition(Condition condition, Integer currentValue) {
        Integer targetValue = condition.getValue();

        if (currentValue == null || targetValue == null) {
            log.warn("Cannot evaluate condition with null values: current={}, target={}",
                    currentValue, targetValue);
            return false;
        }

        log.info("Evaluating: {} {} {}", currentValue, condition.getOperation(), targetValue);

        return switch (condition.getOperation()) {
            case EQUALS -> currentValue.equals(targetValue);
            case GREATER_THAN -> currentValue > targetValue;
            case LOWER_THAN -> currentValue < targetValue;
        };
    }

    private void executeScenarioActions(Scenario scenario) {
        scenarioActionRepository.findByScenario(scenario)
                .forEach(action -> {
                    log.info("🚀 Executing action: scenario={}, hub={}, sensor={}, type={}, value={}",
                            scenario.getName(),
                            scenario.getHubId(),
                            action.getSensor().getId(),
                            action.getAction().getType(),
                            action.getAction().getValue());
                    hubRouterClient.sendDeviceRequest(action);
                });
    }
}
