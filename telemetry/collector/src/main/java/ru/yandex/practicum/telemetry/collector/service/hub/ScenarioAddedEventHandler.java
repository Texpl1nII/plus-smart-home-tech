package ru.yandex.practicum.telemetry.collector.service.hub;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaClientProducer;
import ru.yandex.practicum.telemetry.collector.dto.hub.*;

import java.util.List;

@Slf4j
@Component
public class ScenarioAddedEventHandler extends BaseHubEventHandler<ScenarioAddedEventAvro> {
    public ScenarioAddedEventHandler(KafkaClientProducer producer) {
        super(producer);
    }

    @Override
    protected ScenarioAddedEventAvro mapToAvro(HubEvent event) {
        ScenarioAddedEvent scenarioAddedEvent = (ScenarioAddedEvent) event;

        return ScenarioAddedEventAvro.newBuilder()
                .setName(scenarioAddedEvent.getName())
                .setConditions(mapConditionsToAvro(scenarioAddedEvent.getConditions()))
                .setActions(mapActionsToAvro(scenarioAddedEvent.getActions()))
                .build();
    }

    private List<ScenarioConditionAvro> mapConditionsToAvro(List<ScenarioCondition> conditions) {
        return conditions.stream()
                .map(this::mapConditionToAvro)
                .toList();
    }

    private ScenarioConditionAvro mapConditionToAvro(ScenarioCondition condition) {
        return ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()))
                .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                .setValue(condition.getValue())  // value теперь int, просто передаем
                .build();
    }

    private List<DeviceActionAvro> mapActionsToAvro(List<DeviceAction> actions) {
        return actions.stream()
                .map(this::mapActionToAvro)
                .toList();
    }

    private DeviceActionAvro mapActionToAvro(DeviceAction action) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(ActionTypeAvro.valueOf(action.getType().name()))
                .setValue(action.getValue())  // value теперь int, просто передаем
                .build();
    }

    @Override
    public HubEventType getMessageType() {
        return HubEventType.SCENARIO_ADDED;
    }
}