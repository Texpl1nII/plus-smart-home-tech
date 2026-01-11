package ru.yandex.practicum.telemetry.collector.converter;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.collector.dto.hub.*;
import ru.yandex.practicum.telemetry.collector.dto.sensor.*;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class EventConverter {

    public HubEventAvro convertToAvro(HubEvent event) {
        // Обрати внимание: в схеме поле называется hub_id, а не hubId!
        HubEventAvro.Builder builder = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())  // Используй именно setHubId (Avro сам конвертирует)
                .setTimestamp(event.getTimestamp().toEpochMilli());

        // Обработка разных типов событий хаба
        if (event instanceof DeviceAddedEvent) {
            DeviceAddedEvent deviceEvent = (DeviceAddedEvent) event;
            builder.setPayload(DeviceAddedEventAvro.newBuilder()
                    .setId(deviceEvent.getId())
                    .setType(convertDeviceType(deviceEvent.getDeviceType()))
                    .build());
        }
        else if (event instanceof DeviceRemovedEvent) {
            DeviceRemovedEvent deviceEvent = (DeviceRemovedEvent) event;
            builder.setPayload(DeviceRemovedEventAvro.newBuilder()
                    .setId(deviceEvent.getId())
                    .build());
        }
        else if (event instanceof ScenarioAddedEvent) {
            ScenarioAddedEvent scenarioEvent = (ScenarioAddedEvent) event;
            builder.setPayload(ScenarioAddedEventAvro.newBuilder()
                    .setName(scenarioEvent.getName())
                    .setConditions(convertConditions(scenarioEvent.getConditions()))
                    .setActions(convertActions(scenarioEvent.getActions()))
                    .build());
        }
        else if (event instanceof ScenarioRemovedEvent) {
            ScenarioRemovedEvent scenarioEvent = (ScenarioRemovedEvent) event;
            builder.setPayload(ScenarioRemovedEventAvro.newBuilder()
                    .setName(scenarioEvent.getName())
                    .build());
        } else {
            throw new IllegalArgumentException("Unsupported hub event type: " + event.getClass().getSimpleName());
        }

        return builder.build();
    }

    public SensorEventAvro convertToAvro(SensorEvent event) {
        // Обрати внимание: в схеме поле называется hubId (camelCase), а не hub_id!
        SensorEventAvro.Builder builder = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())  // Используй именно setHubId
                .setTimestamp(event.getTimestamp().toEpochMilli());

        // Обработка разных типов событий сенсоров
        if (event instanceof LightSensorEvent) {
            LightSensorEvent lightEvent = (LightSensorEvent) event;
            builder.setPayload(LightSensorAvro.newBuilder()
                    .setLinkQuality(lightEvent.getLinkQuality())
                    .setLuminosity(lightEvent.getLuminosity())
                    .build());
        }
        else if (event instanceof ClimateSensorEvent) {
            ClimateSensorEvent climateEvent = (ClimateSensorEvent) event;
            builder.setPayload(ClimateSensorAvro.newBuilder()
                    .setTemperatureC(climateEvent.getTemperatureC())
                    .setHumidity(climateEvent.getHumidity())
                    .setCo2Level(climateEvent.getCo2Level())
                    .build());
        }
        else if (event instanceof TemperatureSensorEvent) {
            TemperatureSensorEvent tempEvent = (TemperatureSensorEvent) event;
            builder.setPayload(TemperatureSensorAvro.newBuilder()
                    .setTemperatureC(tempEvent.getTemperatureC())
                    .setTemperatureF(tempEvent.getTemperatureF())
                    .build());
        }
        else if (event instanceof MotionSensorEvent) {
            MotionSensorEvent motionEvent = (MotionSensorEvent) event;
            builder.setPayload(MotionSensorAvro.newBuilder()
                    .setLinkQuality(motionEvent.getLinkQuality())
                    .setMotion(motionEvent.isMotion())
                    .setVoltage(motionEvent.getVoltage())
                    .build());
        }
        else if (event instanceof SwitchSensorEvent) {
            SwitchSensorEvent switchEvent = (SwitchSensorEvent) event;
            builder.setPayload(SwitchSensorAvro.newBuilder()
                    .setState(switchEvent.isState())
                    .build());
        } else {
            throw new IllegalArgumentException("Unsupported sensor event type: " + event.getClass().getSimpleName());
        }

        return builder.build();
    }

    // Вспомогательные методы конвертации
    private DeviceTypeAvro convertDeviceType(DeviceType type) {
        return DeviceTypeAvro.valueOf(type.name());
    }

    private List<ScenarioConditionAvro> convertConditions(List<ScenarioCondition> conditions) {
        if (conditions == null) return List.of();

        return conditions.stream().map(condition ->
                ScenarioConditionAvro.newBuilder()
                        .setSensorId(condition.getSensorId())
                        .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                        .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()))
                        .setValue(condition.getValue())  // Автоматическая конвертация Object → union
                        .build()
        ).collect(Collectors.toList());
    }

    private List<DeviceActionAvro> convertActions(List<DeviceAction> actions) {
        if (actions == null) return List.of();

        return actions.stream().map(action ->
                DeviceActionAvro.newBuilder()
                        .setSensorId(action.getSensorId())
                        .setType(ActionTypeAvro.valueOf(action.getType().name()))
                        .setValue(action.getValue())
                        .build()
        ).collect(Collectors.toList());
    }
}
