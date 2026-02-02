package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.analyzer.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioAction;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioCondition;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioActionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;

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
    public void handle(SensorsSnapshotAvro sensorsSnapshotAvro) {
        String hubId = sensorsSnapshotAvro.getHubId();
        Map<String, SensorStateAvro> sensorStateMap = sensorsSnapshotAvro.getSensorsState();

        log.info("📸 SNAPSHOT RECEIVED: hub={}, sensors={}", hubId, sensorStateMap.size());

        // Детальное логирование каждого сенсора
        sensorStateMap.forEach((sensorId, state) -> {
            Object data = state.getData();
            log.debug("Sensor {}: type={}, data={}",
                    sensorId,
                    data.getClass().getSimpleName(),
                    data.toString());
        });

        // Получаем все сценарии для этого хаба
        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);
        log.info("Found {} scenarios for hub {}", scenarios.size(), hubId);

        if (scenarios.isEmpty()) {
            log.warn("⚠️ No scenarios found for hub {}! Check if ScenarioAddedEventHandler works.", hubId);
            return;
        }

        // Проверяем каждый сценарий
        for (Scenario scenario : scenarios) {
            log.info("🔍 Checking scenario: '{}' (hub: {})", scenario.getName(), scenario.getHubId());

            boolean allConditionsMet = checkAllConditions(scenario, sensorStateMap);

            if (allConditionsMet) {
                log.info("✅✅✅ ALL CONDITIONS MET! Executing scenario: '{}'", scenario.getName());
                executeScenarioActions(scenario);
            } else {
                log.info("❌ Some conditions not met for scenario: '{}'", scenario.getName());
            }
        }
    }

    private boolean checkAllConditions(Scenario scenario, Map<String, SensorStateAvro> sensorStateMap) {
        List<ScenarioCondition> conditions = scenarioConditionRepository.findByScenario(scenario);

        if (conditions.isEmpty()) {
            log.warn("Scenario '{}' has no conditions!", scenario.getName());
            return false;
        }

        log.debug("Checking {} conditions for scenario '{}'", conditions.size(), scenario.getName());

        for (ScenarioCondition sc : conditions) {
            String sensorId = sc.getSensor().getId();
            Condition condition = sc.getCondition();

            if (!checkSingleCondition(condition, sensorId, sensorStateMap)) {
                log.info("❌ Condition NOT met: sensor={}, type={}, operation={}, targetValue={}",
                        sensorId, condition.getType(), condition.getOperation(), condition.getValue());
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
            log.warn("Could not extract value for sensor {} with type {}",
                    sensorId, condition.getType());
            return false;
        }

        Integer targetValue = condition.getValue();
        if (targetValue == null) {
            log.warn("Target value is null for condition: sensor={}, type={}",
                    sensorId, condition.getType());
            return false;
        }

        boolean result = evaluateCondition(condition, currentValue);
        log.debug("Condition check: sensor={}, type={}, current={}, target={}, operation={}, result={}",
                sensorId, condition.getType(), currentValue, targetValue,
                condition.getOperation(), result ? "PASS" : "FAIL");

        return result;
    }

    private Integer extractValue(SensorStateAvro sensorState, ConditionTypeAvro type) {
        Object data = sensorState.getData();

        try {
            return switch (type) {
                case MOTION -> {
                    MotionSensorAvro motion = (MotionSensorAvro) data;
                    yield motion.getMotion() ? 1 : 0;
                }
                case LUMINOSITY -> {
                    LightSensorAvro light = (LightSensorAvro) data;
                    yield light.getLuminosity();
                }
                case SWITCH -> {
                    SwitchSensorAvro sw = (SwitchSensorAvro) data;
                    yield sw.getState() ? 1 : 0;
                }
                case TEMPERATURE -> {
                    if (data instanceof ClimateSensorAvro) {
                        ClimateSensorAvro climate = (ClimateSensorAvro) data;
                        yield climate.getTemperatureC();
                    } else if (data instanceof TemperatureSensorAvro) {
                        TemperatureSensorAvro temp = (TemperatureSensorAvro) data;
                        yield temp.getTemperatureC();
                    } else {
                        log.error("Unsupported data type for TEMPERATURE: {}",
                                data.getClass().getSimpleName());
                        yield null;
                    }
                }
                case CO2LEVEL -> {
                    ClimateSensorAvro climate = (ClimateSensorAvro) data;
                    yield climate.getCo2Level();
                }
                case HUMIDITY -> {
                    ClimateSensorAvro climate = (ClimateSensorAvro) data;
                    yield climate.getHumidity();
                }
            };
        } catch (ClassCastException e) {
            log.error("ClassCastException for type {}: {}", type, e.getMessage());
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
        List<ScenarioAction> actions = scenarioActionRepository.findByScenario(scenario);

        if (actions.isEmpty()) {
            log.warn("Scenario '{}' has no actions to execute!", scenario.getName());
            return;
        }

        log.info("Executing {} actions for scenario '{}'", actions.size(), scenario.getName());

        for (ScenarioAction action : actions) {
            log.info("🚀 Sending action: sensor={}, type={}, value={}",
                    action.getSensor().getId(),
                    action.getAction().getType(),
                    action.getAction().getValue());

            hubRouterClient.sendDeviceRequest(action);
        }
    }
}
