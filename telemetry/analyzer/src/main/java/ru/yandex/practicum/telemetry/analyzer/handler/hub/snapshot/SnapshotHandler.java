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
        log.info("📸 Snapshot received: hub={}, sensors={}",
                sensorsSnapshotAvro.getHubId(),
                sensorsSnapshotAvro.getSensorsState().size());

        Map<String, SensorStateAvro> sensorStateMap = sensorsSnapshotAvro.getSensorsState();
        List<Scenario> scenariosList = scenarioRepository.findByHubId(sensorsSnapshotAvro.getHubId());

        log.info("Found {} scenarios for hub {}", scenariosList.size(), sensorsSnapshotAvro.getHubId());

        for (Scenario scenario : scenariosList) {
            boolean shouldExecute = checkAllConditions(scenario, sensorStateMap);
            if (shouldExecute) {
                log.info("✅ Executing scenario: {}", scenario.getName());
                executeScenarioActions(scenario);
            }
        }
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
        SensorStateAvro sensorState = sensorStateMap.get(sensorId);
        if (sensorState == null) {
            log.warn("Sensor {} not found in snapshot", sensorId);
            return false;
        }

        Integer currentValue = extractValue(sensorState, condition.getType());
        if (currentValue == null) {
            return false;
        }

        return evaluateCondition(condition, currentValue);
    }

    private Integer extractValue(SensorStateAvro sensorState, ConditionTypeAvro type) {
        try {
            Object data = sensorState.getData();
            log.debug("Extracting value for type {} from data type: {}",
                    type, data.getClass().getSimpleName());

            return switch (type) {
                case MOTION -> {
                    MotionSensorAvro motion = (MotionSensorAvro) data;
                    int value = motion.getMotion() ? 1 : 0;
                    log.debug("Motion value: {} -> {}", motion.getMotion(), value);
                    yield value;
                }
                case LUMINOSITY -> {
                    LightSensorAvro light = (LightSensorAvro) data;
                    log.debug("Luminosity value: {}", light.getLuminosity());
                    yield light.getLuminosity();
                }
                case SWITCH -> {
                    SwitchSensorAvro sw = (SwitchSensorAvro) data;
                    int value = sw.getState() ? 1 : 0;
                    log.debug("Switch value: {} -> {}", sw.getState(), value);
                    yield value;
                }
                case TEMPERATURE -> {
                    ClimateSensorAvro climate = (ClimateSensorAvro) data;
                    log.debug("Temperature value: {}", climate.getTemperatureC());
                    yield climate.getTemperatureC();
                }
                case CO2LEVEL -> {
                    ClimateSensorAvro climate = (ClimateSensorAvro) data;
                    log.debug("CO2 level: {}", climate.getCo2Level());
                    yield climate.getCo2Level();
                }
                case HUMIDITY -> {
                    ClimateSensorAvro climate = (ClimateSensorAvro) data;
                    log.debug("Humidity: {}", climate.getHumidity());
                    yield climate.getHumidity();
                }
            };
        } catch (ClassCastException e) {
            log.error("❌ Type mismatch! Condition type: {}, Data type: {}",
                    type, sensorState.getData().getClass().getSimpleName(), e);
            return null;
        } catch (Exception e) {
            log.error("Error extracting value for type {}", type, e);
            return null;
        }
    }

    private boolean evaluateCondition(Condition condition, Integer currentValue) {
        Integer targetValue = condition.getValue();

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
