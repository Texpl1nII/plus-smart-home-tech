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
        try {
            ScenarioAddedEvent scenarioAddedEvent = (ScenarioAddedEvent) event;

            ScenarioAddedEventAvro.Builder builder = ScenarioAddedEventAvro.newBuilder()
                    .setName(scenarioAddedEvent.getName());

            // Обработка conditions
            if (scenarioAddedEvent.getConditions() != null && !scenarioAddedEvent.getConditions().isEmpty()) {
                builder.setConditions(mapConditionsToAvro(scenarioAddedEvent.getConditions()));
            } else {
                builder.setConditions(List.of());
            }

            // Обработка actions
            if (scenarioAddedEvent.getActions() != null && !scenarioAddedEvent.getActions().isEmpty()) {
                builder.setActions(mapActionsToAvro(scenarioAddedEvent.getActions()));
            } else {
                builder.setActions(List.of());
            }

            return builder.build();
        } catch (Exception e) {
            log.error("Error mapping ScenarioAddedEvent: hubId={}", event.getHubId(), e);
            throw new RuntimeException("Failed to map ScenarioAddedEvent to Avro", e);
        }
    }

    private List<ScenarioConditionAvro> mapConditionsToAvro(List<ScenarioCondition> conditions) {
        return conditions.stream()
                .map(this::mapConditionToAvro)
                .toList();
    }

    private ScenarioConditionAvro mapConditionToAvro(ScenarioCondition condition) {
        try {
            ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                    .setSensorId(condition.getSensorId())
                    .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()))
                    .setType(ConditionTypeAvro.valueOf(condition.getType().name()));

            // Простая обработка value - теперь это Integer
            Integer value = condition.getValue();
            if (value != null) {
                builder.setValue(value);
            } else {
                builder.setValue(null);
            }

            return builder.build();
        } catch (Exception e) {
            log.error("Error mapping condition: sensorId={}", condition.getSensorId(), e);
            throw new RuntimeException("Failed to map condition to Avro", e);
        }
    }

    private List<DeviceActionAvro> mapActionsToAvro(List<DeviceAction> actions) {
        return actions.stream()
                .map(this::mapActionToAvro)
                .toList();
    }

    private DeviceActionAvro mapActionToAvro(DeviceAction action) {
        try {
            DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                    .setSensorId(action.getSensorId())
                    .setType(ActionTypeAvro.valueOf(action.getType().name()))
                    .setValue(action.getValue());  // теперь value всегда есть (int примитив)

            return builder.build();
        } catch (Exception e) {
            log.error("Error mapping action: sensorId={}", action.getSensorId(), e);
            throw new RuntimeException("Failed to map action to Avro", e);
        }
    }

    @Override
    public HubEventType getMessageType() {
        return HubEventType.SCENARIO_ADDED;
    }
}