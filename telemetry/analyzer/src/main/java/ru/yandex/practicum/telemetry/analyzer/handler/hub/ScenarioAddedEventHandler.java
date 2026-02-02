package ru.yandex.practicum.telemetry.analyzer.handler.hub;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.telemetry.analyzer.handler.HubEventHandler;
import ru.yandex.practicum.telemetry.analyzer.model.*;
import ru.yandex.practicum.telemetry.analyzer.repository.*;

import java.util.List;
import java.util.Optional;

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
        ScenarioAddedEventAvro scenarioAddedEventAvro = (ScenarioAddedEventAvro) event.getPayload();
        List<String> conditionSensorIds = scenarioAddedEventAvro.getConditions().stream()
                .map(ScenarioConditionAvro::getSensorId)
                .toList();
        List<String> actionSensorIds = scenarioAddedEventAvro.getActions().stream()
                .map(DeviceActionAvro::getSensorId)
                .toList();

        if (!sensorRepository.existsByIdInAndHubId(conditionSensorIds, event.getHubId()) ||
                !sensorRepository.existsByIdInAndHubId(actionSensorIds, event.getHubId())
        ) {
            throw new IllegalArgumentException("Устройства не найдены");
        }

        Optional<Scenario> scenario = scenarioRepository.findByHubIdAndName(scenarioAddedEventAvro.getName(), event.getHubId());
        scenario.ifPresent(prevScenario -> {
            scenarioActionRepository.deleteByScenario(prevScenario);
            scenarioConditionRepository.deleteByScenario(prevScenario);
            scenarioRepository.deleteByHubIdAndName(
                    prevScenario.getHubId(),
                    prevScenario.getName()
            );
        });

        Scenario scenarioToUpload = Scenario.builder()
                .name(scenarioAddedEventAvro.getName())
                .hubId(event.getHubId())
                .build();

        scenarioRepository.save(scenarioToUpload);

        saveConditions(scenarioToUpload, event, scenarioAddedEventAvro);
        saveActions(scenarioToUpload, event, scenarioAddedEventAvro);
    }

    private void saveConditions(Scenario scenario, HubEventAvro event, ScenarioAddedEventAvro avro) {
        for (ScenarioConditionAvro conditionAvro : avro.getConditions()) {
            Sensor sensor = sensorRepository.findById(conditionAvro.getSensorId())
                    .orElseThrow(() -> new IllegalArgumentException("Сенсор не найден"));

            Condition condition = conditionRepository.save(
                    Condition.builder()
                            .type(conditionAvro.getType())
                            .operation(conditionAvro.getOperation())
                            .value(asInteger(conditionAvro.getValue()))
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
        }
    }

    private void saveActions(Scenario scenario, HubEventAvro event, ScenarioAddedEventAvro avro) {
        for (DeviceActionAvro actionAvro : avro.getActions()) {
            Sensor sensor = sensorRepository.findById(actionAvro.getSensorId())
                    .orElseThrow(() -> new IllegalArgumentException("Сенсор не найден"));

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
        }
    }

    private Integer asInteger(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Boolean) {
            return ((Boolean) value) ? 1 : 0;
        }
        throw new IllegalArgumentException("Неподдерживаемый тип значения: " + value.getClass());
    }
}