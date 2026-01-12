package ru.yandex.practicum.telemetry.collector.service.hub;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaClientProducer;
import ru.yandex.practicum.telemetry.collector.dto.hub.*;

import java.util.List;

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
        if (conditions == null) {
            return List.of();
        }

        return conditions.stream()
                .map(this::mapConditionToAvro)
                .toList();
    }

    private ScenarioConditionAvro mapConditionToAvro(ScenarioCondition condition) {
        ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()))
                .setType(ConditionTypeAvro.valueOf(condition.getType().name()));

        // Обработка value (может быть null, int или boolean)
        if (condition.getValue() == null) {
            builder.setValue(null);
        } else if (condition.getValue() instanceof Integer) {
            builder.setValue((Integer) condition.getValue());
        } else if (condition.getValue() instanceof Boolean) {
            builder.setValue((Boolean) condition.getValue());
        }

        return builder.build();
    }

    private List<DeviceActionAvro> mapActionsToAvro(List<DeviceAction> actions) {
        if (actions == null) {
            return List.of();
        }

        return actions.stream()
                .map(this::mapActionToAvro)
                .toList();
    }

    private DeviceActionAvro mapActionToAvro(DeviceAction action) {
        DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(ActionTypeAvro.valueOf(action.getType().name()));

        if (action.getValue() != null) {
            builder.setValue(action.getValue());
        }

        return builder.build();
    }

    @Override
    public HubEventType getMessageType() {
        return HubEventType.SCENARIO_ADDED;
    }
}