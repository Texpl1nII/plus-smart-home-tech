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

        // Проверяем существование сценария
        Optional<Scenario> existingScenario = scenarioRepository
                .findByHubIdAndName(event.getHubId(), scenarioEvent.getName());

        // Удаляем старый сценарий если существует
        existingScenario.ifPresent(scenario -> {
            scenarioActionRepository.deleteByScenario(scenario);
            scenarioConditionRepository.deleteByScenario(scenario);
            scenarioRepository.delete(scenario);
            log.info("Deleted existing scenario: {} for hub: {}",
                    scenario.getName(), scenario.getHubId());
        });

        // Создаем новый сценарий
        Scenario scenario = Scenario.builder()
                .hubId(event.getHubId())
                .name(scenarioEvent.getName())
                .build();

        scenario = scenarioRepository.save(scenario);

        // Сохраняем условия
        for (ScenarioConditionAvro conditionAvro : scenarioEvent.getConditions()) {
            saveCondition(scenario, conditionAvro);
        }

        // Сохраняем действия
        for (DeviceActionAvro actionAvro : scenarioEvent.getActions()) {
            saveAction(scenario, actionAvro);
        }

        log.info("Scenario added: {} for hub: {} with {} conditions and {} actions",
                scenarioEvent.getName(), event.getHubId(),
                scenarioEvent.getConditions().size(), scenarioEvent.getActions().size());
    }

    private void saveCondition(Scenario scenario, ScenarioConditionAvro conditionAvro) {
        // Проверяем существование сенсора
        Sensor sensor = sensorRepository.findByIdAndHubId(
                        conditionAvro.getSensorId(), scenario.getHubId())
                .orElseThrow(() -> new IllegalArgumentException(
                        "Sensor not found: " + conditionAvro.getSensorId()));

        // Создаем условие
        Condition condition = Condition.builder()
                .type(convertConditionType(conditionAvro.getType()))
                .operation(convertConditionOperation(conditionAvro.getOperation()))
                .value(asInteger(conditionAvro.getValue()))
                .build();

        condition = conditionRepository.save(condition);

        // Создаем связь сценария с условием
        ScenarioCondition scenarioCondition = ScenarioCondition.builder()
                .id(new ScenarioConditionId(scenario.getId(), sensor.getId(), condition.getId()))
                .scenario(scenario)
                .sensor(sensor)
                .condition(condition)
                .build();

        scenarioConditionRepository.save(scenarioCondition);
    }

    private void saveAction(Scenario scenario, DeviceActionAvro actionAvro) {
        // Проверяем существование сенсора
        Sensor sensor = sensorRepository.findByIdAndHubId(
                        actionAvro.getSensorId(), scenario.getHubId())
                .orElseThrow(() -> new IllegalArgumentException(
                        "Sensor not found: " + actionAvro.getSensorId()));

        // Создаем действие
        Action action = Action.builder()
                .type(convertActionType(actionAvro.getType()))
                .value(actionAvro.getValue())
                .build();

        action = actionRepository.save(action);

        // Создаем связь сценария с действием
        ScenarioAction scenarioAction = ScenarioAction.builder()
                .id(new ScenarioActionId(scenario.getId(), sensor.getId(), action.getId()))
                .scenario(scenario)
                .sensor(sensor)
                .action(action)
                .build();

        scenarioActionRepository.save(scenarioAction);
    }

    private ConditionType convertConditionType(ConditionTypeAvro type) {
        return switch (type) {
            case MOTION -> ConditionType.MOTION;
            case LUMINOSITY -> ConditionType.LUMINOSITY;
            case SWITCH -> ConditionType.SWITCH;
            case TEMPERATURE -> ConditionType.TEMPERATURE;
            case CO2LEVEL -> ConditionType.CO2LEVEL;
            case HUMIDITY -> ConditionType.HUMIDITY;
        };
    }

    private ConditionOperation convertConditionOperation(ConditionOperationAvro operation) {
        return switch (operation) {
            case EQUALS -> ConditionOperation.EQUALS;
            case GREATER_THAN -> ConditionOperation.GREATER_THAN;
            case LOWER_THAN -> ConditionOperation.LOWER_THAN;
        };
    }

    private ActionType convertActionType(ActionTypeAvro type) {
        return switch (type) {
            case ACTIVATE -> ActionType.ACTIVATE;
            case DEACTIVATE -> ActionType.DEACTIVATE;
            case INVERSE -> ActionType.INVERSE;
            case SET_VALUE -> ActionType.SET_VALUE;
        };
    }

    private Integer asInteger(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Boolean) {
            return ((Boolean) value) ? 1 : 0;
        }
        throw new IllegalArgumentException("Unsupported value type: " +
                (value != null ? value.getClass().getName() : "null"));
    }
}
