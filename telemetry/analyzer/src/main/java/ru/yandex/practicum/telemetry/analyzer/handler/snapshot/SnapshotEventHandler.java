package ru.yandex.practicum.telemetry.analyzer.handler.snapshot;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.analyzer.grpc.HubRouterClient;
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
public class SnapshotEventHandler {

    private final ScenarioRepository scenarioRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;
    private final HubRouterClient hubRouterClient;

    @Transactional(readOnly = true)
    public void handle(SensorsSnapshotAvro sensorsSnapshotAvro) {
        Map<String, SensorStateAvro> sensorStateMap = sensorsSnapshotAvro.getSensorsState();
        List<Scenario> scenariosList = scenarioRepository.findByHubId(sensorsSnapshotAvro.getHubId());

        scenariosList.stream()
                .filter(scenario -> handleScenario(scenario, sensorStateMap))
                .forEach(this::sendScenarioAction);
    }

    private boolean handleScenario(Scenario scenario, Map<String, SensorStateAvro> sensorStateMap) {
        List<ScenarioCondition> scenarioConditions =
                scenarioConditionRepository.findByScenario(scenario);

        return scenarioConditions.stream()
                .noneMatch(sc -> !checkCondition(sc.getCondition(),
                        sc.getSensor().getId(),
                        sensorStateMap));
    }

    private boolean handleOperation(Condition condition, Integer currentValue) {
        Integer targetValue = condition.getValue();
        return switch (condition.getOperation()) {
            case EQUALS -> targetValue.equals(currentValue);
            case GREATER_THAN -> currentValue > targetValue;
            case LOWER_THAN -> currentValue < targetValue;
        };
    }

    private boolean checkCondition(Condition condition, String sensorId,
                                   Map<String, SensorStateAvro> sensorStateMap) {

        SensorStateAvro sensorState = sensorStateMap.get(sensorId);
        if (sensorState == null) {
            return false;
        }
        return switch (condition.getType()) {
            case MOTION -> {
                MotionSensorAvro motion = (MotionSensorAvro) sensorState.getData();
                yield handleOperation(condition, motion.getMotion() ? 1 : 0);
            }
            case LUMINOSITY -> {
                LightSensorAvro light = (LightSensorAvro) sensorState.getData();
                yield handleOperation(condition, light.getLuminosity());
            }
            case SWITCH -> {
                SwitchSensorAvro sw = (SwitchSensorAvro) sensorState.getData();
                yield handleOperation(condition, sw.getState() ? 1 : 0);
            }
            case TEMPERATURE -> {
                ClimateSensorAvro climate = (ClimateSensorAvro) sensorState.getData();
                yield handleOperation(condition, climate.getTemperatureC());
            }
            case CO2LEVEL -> {
                ClimateSensorAvro climate = (ClimateSensorAvro) sensorState.getData();
                yield handleOperation(condition, climate.getCo2Level());
            }
            case HUMIDITY -> {
                ClimateSensorAvro climate = (ClimateSensorAvro) sensorState.getData();
                yield handleOperation(condition, climate.getHumidity());
            }
        };
    }

    private void sendScenarioAction(Scenario scenario) {
        scenarioActionRepository.findByScenario(scenario).forEach(hubRouterClient::sendDeviceRequest);
    }
}