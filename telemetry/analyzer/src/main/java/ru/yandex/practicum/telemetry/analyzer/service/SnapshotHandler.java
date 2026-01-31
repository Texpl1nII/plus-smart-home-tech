package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.analyzer.entity.Condition;
import ru.yandex.practicum.telemetry.analyzer.entity.Scenario;
import ru.yandex.practicum.telemetry.analyzer.entity.ScenarioAction;
import ru.yandex.practicum.telemetry.analyzer.entity.ScenarioCondition;
import ru.yandex.practicum.telemetry.analyzer.grpc.HubRouterClient;
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

        if (scenarios.isEmpty()) {
            log.debug("No scenarios found for hub: {}", snapshot.getHubId());
            return;
        }

        scenarios.stream()
                .filter(scenario -> checkScenarioConditions(scenario, sensorStates))
                .forEach(this::executeScenarioActions);
    }

    private boolean checkScenarioConditions(Scenario scenario,
                                            Map<String, SensorStateAvro> sensorStates) {
        List<ScenarioCondition> conditions =
                scenarioConditionRepository.findByScenario(scenario);

        return conditions.stream()
                .allMatch(condition -> checkCondition(condition, sensorStates));
    }

    private boolean checkCondition(ScenarioCondition scenarioCondition,
                                   Map<String, SensorStateAvro> sensorStates) {
        Condition condition = scenarioCondition.getCondition();
        String sensorId = scenarioCondition.getSensor().getId();
        SensorStateAvro sensorState = sensorStates.get(sensorId);

        if (sensorState == null) {
            log.debug("Sensor {} not found in snapshot", sensorId);
            return false;
        }

        Integer currentValue = extractSensorValue(condition.getType(), sensorState);
        if (currentValue == null) {
            return false;
        }

        return checkConditionOperation(condition.getOperation(),
                currentValue, condition.getValue());
    }

    private Integer extractSensorValue(ConditionTypeAvro conditionType, // ИСПРАВЛЕНО: ConditionTypeAvro
                                       SensorStateAvro sensorState) {
        Object data = sensorState.getData();

        // ИСПРАВЛЕНО: используем ConditionTypeAvro напрямую
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
            default -> null;
        };
    }

    private boolean checkConditionOperation(ConditionOperationAvro operation, // ИСПРАВЛЕНО: ConditionOperationAvro
                                            Integer currentValue, Integer targetValue) {
        // ИСПРАВЛЕНО: используем ConditionOperationAvro напрямую
        return switch (operation) {
            case EQUALS -> currentValue.equals(targetValue);
            case GREATER_THAN -> currentValue > targetValue;
            case LOWER_THAN -> currentValue < targetValue;
        };
    }

    private void executeScenarioActions(Scenario scenario) {
        List<ScenarioAction> actions = scenarioActionRepository.findByScenario(scenario);

        actions.forEach(scenarioAction -> {
            try {
                hubRouterClient.sendDeviceRequest(scenarioAction);
                log.info("Executed action for scenario: {} on sensor: {}",
                        scenario.getName(), scenarioAction.getSensor().getId());
            } catch (Exception e) {
                log.error("Failed to execute action for scenario: {} on sensor: {}",
                        scenario.getName(), scenarioAction.getSensor().getId(), e);
            }
        });
    }
}
