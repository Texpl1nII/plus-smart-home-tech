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

            // Детальное логирование
            log.info("=== START Mapping ScenarioAddedEvent ===");
            log.info("HubId: {}", scenarioAddedEvent.getHubId());
            log.info("Name: {}", scenarioAddedEvent.getName());
            log.info("Timestamp: {}", scenarioAddedEvent.getTimestamp());

            if (scenarioAddedEvent.getConditions() != null) {
                log.info("Conditions count: {}", scenarioAddedEvent.getConditions().size());
                for (int i = 0; i < scenarioAddedEvent.getConditions().size(); i++) {
                    ScenarioCondition cond = scenarioAddedEvent.getConditions().get(i);
                    log.info("Condition [{}]: sensorId={}, type={}, operation={}, value={} (class: {})",
                            i, cond.getSensorId(), cond.getType(), cond.getOperation(),
                            cond.getValue(),
                            cond.getValue() != null ? cond.getValue().getClass().getSimpleName() : "null");
                }
            } else {
                log.info("Conditions: null or empty");
            }

            if (scenarioAddedEvent.getActions() != null) {
                log.info("Actions count: {}", scenarioAddedEvent.getActions().size());
                for (int i = 0; i < scenarioAddedEvent.getActions().size(); i++) {
                    DeviceAction action = scenarioAddedEvent.getActions().get(i);
                    log.info("Action [{}]: sensorId={}, type={}, value={}",
                            i, action.getSensorId(), action.getType(), action.getValue());
                }
            } else {
                log.info("Actions: null or empty");
            }
            log.info("=== END Mapping ScenarioAddedEvent ===");

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
            log.error("Error mapping ScenarioAddedEvent: hubId={}, type={}",
                    event.getHubId(), event.getType(), e);
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

            // Обработка value - расширенная версия
            Object value = condition.getValue();
            if (value == null) {
                builder.setValue(null);
            } else if (value instanceof Integer) {
                builder.setValue((Integer) value);
            } else if (value instanceof Boolean) {
                builder.setValue((Boolean) value);
            } else if (value instanceof Long) {
                builder.setValue(((Long) value).intValue());
            } else if (value instanceof Double) {
                // Для температур и других значений с плавающей точкой
                builder.setValue(((Double) value).intValue());
            } else if (value instanceof Float) {
                builder.setValue(((Float) value).intValue());
            } else if (value instanceof String) {
                // Пробуем парсить строку в число
                try {
                    String strValue = (String) value;
                    if (strValue.contains(".")) {
                        Double doubleValue = Double.parseDouble(strValue);
                        builder.setValue(doubleValue.intValue());
                    } else {
                        Integer intValue = Integer.parseInt(strValue);
                        builder.setValue(intValue);
                    }
                } catch (NumberFormatException e) {
                    log.error("Cannot parse string value '{}' to integer for condition: {}",
                            value, condition);
                    builder.setValue(null);
                }
            } else {
                log.error("Unsupported condition value type: {} for condition: {}",
                        value.getClass(), condition);
                throw new IllegalArgumentException(
                        "Unsupported value type: " + value.getClass().getName() +
                                " for condition. Value: " + value);
            }

            return builder.build();
        } catch (Exception e) {
            log.error("Error mapping condition: sensorId={}, type={}, operation={}, value={}",
                    condition.getSensorId(), condition.getType(),
                    condition.getOperation(), condition.getValue(), e);
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
                    .setType(ActionTypeAvro.valueOf(action.getType().name()));

            // Обработка value - может быть null
            Integer value = action.getValue();
            if (value != null) {
                builder.setValue(value);
            } else {
                // Для Avro nullable поля нужно явно установить null
                builder.setValue(null);
            }

            return builder.build();
        } catch (Exception e) {
            log.error("Error mapping action: sensorId={}, type={}, value={}",
                    action.getSensorId(), action.getType(), action.getValue(), e);
            throw new RuntimeException("Failed to map action to Avro", e);
        }
    }

    @Override
    public HubEventType getMessageType() {
        return HubEventType.SCENARIO_ADDED;
    }
}