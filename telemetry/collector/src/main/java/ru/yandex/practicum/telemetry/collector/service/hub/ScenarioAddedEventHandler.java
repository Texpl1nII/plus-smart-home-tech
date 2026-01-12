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
            log.debug("Mapping ScenarioAddedEvent: name={}, conditions={}, actions={}",
                    scenarioAddedEvent.getName(),
                    scenarioAddedEvent.getConditions() != null ? scenarioAddedEvent.getConditions().size() : 0,
                    scenarioAddedEvent.getActions() != null ? scenarioAddedEvent.getActions().size() : 0);

            ScenarioAddedEventAvro.Builder builder = ScenarioAddedEventAvro.newBuilder()
                    .setName(scenarioAddedEvent.getName());

            // Обработка conditions (может быть null)
            if (scenarioAddedEvent.getConditions() != null && !scenarioAddedEvent.getConditions().isEmpty()) {
                builder.setConditions(mapConditionsToAvro(scenarioAddedEvent.getConditions()));
            } else {
                builder.setConditions(List.of());
            }

            // Обработка actions (может быть null)
            if (scenarioAddedEvent.getActions() != null && !scenarioAddedEvent.getActions().isEmpty()) {
                builder.setActions(mapActionsToAvro(scenarioAddedEvent.getActions()));
            } else {
                builder.setActions(List.of());
            }

            return builder.build();
        } catch (Exception e) {
            log.error("Error mapping ScenarioAddedEvent: {}", event, e);
            throw new RuntimeException("Failed to map ScenarioAddedEvent to Avro", e);
        }
    }

    private List<ScenarioConditionAvro> mapConditionsToAvro(List<ScenarioCondition> conditions) {
        return conditions.stream()
                .map(this::mapConditionToAvro)
                .toList();
    }

    private ScenarioConditionAvro mapConditionToAvro(ScenarioCondition condition) {
        ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()))
                .setType(ConditionTypeAvro.valueOf(condition.getType().name()));

        // Обработка value - важный момент!
        Object value = condition.getValue();
        if (value == null) {
            builder.setValue(null);
        } else if (value instanceof Integer) {
            builder.setValue((Integer) value);
        } else if (value instanceof Boolean) {
            builder.setValue((Boolean) value);
        } else {
            log.warn("Unsupported condition value type: {} for condition: {}",
                    value.getClass(), condition);
            builder.setValue(null); // или можно выбросить исключение
        }

        return builder.build();
    }

    private List<DeviceActionAvro> mapActionsToAvro(List<DeviceAction> actions) {
        return actions.stream()
                .map(this::mapActionToAvro)
                .toList();
    }

    private DeviceActionAvro mapActionToAvro(DeviceAction action) {
        DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(ActionTypeAvro.valueOf(action.getType().name()));

        // Обработка value - может быть null
        Integer value = action.getValue();
        if (value != null) {
            builder.setValue(value);
        } else {
            // Для Avro nullable поля нужно явно установить null
            builder.clearValue(); // или builder.setValue(null);
        }

        return builder.build();
    }

    @Override
    public HubEventType getMessageType() {
        return HubEventType.SCENARIO_ADDED;
    }
}