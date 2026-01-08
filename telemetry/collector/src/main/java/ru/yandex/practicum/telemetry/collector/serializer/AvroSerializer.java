package ru.yandex.practicum.telemetry.collector.serializer;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.collector.dto.hub.*;
import ru.yandex.practicum.telemetry.collector.dto.sensor.*;

import java.util.List;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class AvroSerializer {

    public SensorEventAvro convertToAvro(SensorEvent event) {
        SensorEventAvro avro = new SensorEventAvro();
        avro.setId(event.getId());
        avro.setHubId(event.getHubId());
        avro.setTimestamp(event.getTimestamp().toEpochMilli());

        // Устанавливаем payload в зависимости от типа события
        if (event instanceof LightSensorEvent lightEvent) {
            LightSensorAvro lightAvro = LightSensorAvro.newBuilder()
                    .setLinkQuality(lightEvent.getLinkQuality())
                    .setLuminosity(lightEvent.getLuminosity())
                    .build();
            avro.setPayload(lightAvro);

        } else if (event instanceof TemperatureSensorEvent tempEvent) {
            TemperatureSensorAvro tempAvro = TemperatureSensorAvro.newBuilder()
                    .setTemperatureC(tempEvent.getTemperatureC())
                    .setTemperatureF(tempEvent.getTemperatureF())
                    .build();
            avro.setPayload(tempAvro);

        } else if (event instanceof ClimateSensorEvent climateEvent) {
            ClimateSensorAvro climateAvro = ClimateSensorAvro.newBuilder()
                    .setTemperatureC(climateEvent.getTemperatureC())
                    .setHumidity(climateEvent.getHumidity())
                    .setCo2Level(climateEvent.getCo2Level())
                    .build();
            avro.setPayload(climateAvro);

        } else if (event instanceof MotionSensorEvent motionEvent) {
            MotionSensorAvro motionAvro = MotionSensorAvro.newBuilder()
                    .setLinkQuality(motionEvent.getLinkQuality())
                    .setMotion(motionEvent.isMotion())
                    .setVoltage(motionEvent.getVoltage())
                    .build();
            avro.setPayload(motionAvro);

        } else if (event instanceof SwitchSensorEvent switchEvent) {
            SwitchSensorAvro switchAvro = SwitchSensorAvro.newBuilder()
                    .setState(switchEvent.isState())
                    .build();
            avro.setPayload(switchAvro);
        } else {
            throw new IllegalArgumentException("Unknown sensor event type: " + event.getClass());
        }

        return avro;
    }

    public HubEventAvro convertToAvro(HubEvent event) {
        HubEventAvro avro = new HubEventAvro();
        avro.setHubId(event.getHubId());
        avro.setTimestamp(event.getTimestamp().toEpochMilli());

        // Устанавливаем payload в зависимости от типа события
        if (event instanceof DeviceAddedEvent deviceAdded) {
            DeviceAddedEventAvro deviceAvro = DeviceAddedEventAvro.newBuilder()
                    .setId(deviceAdded.getId())
                    .setType(DeviceTypeAvro.valueOf(deviceAdded.getType().name()))
                    .build();
            avro.setPayload(deviceAvro);

        } else if (event instanceof DeviceRemovedEvent deviceRemoved) {
            DeviceRemovedEventAvro deviceAvro = DeviceRemovedEventAvro.newBuilder()
                    .setId(deviceRemoved.getId())
                    .build();
            avro.setPayload(deviceAvro);

        } else if (event instanceof ScenarioAddedEvent scenarioAdded) {
            List<ScenarioConditionAvro> conditions = scenarioAdded.getConditions().stream()
                    .map(this::convertConditionToAvro)
                    .collect(Collectors.toList());

            List<DeviceActionAvro> actions = scenarioAdded.getActions().stream()
                    .map(this::convertActionToAvro)
                    .collect(Collectors.toList());

            ScenarioAddedEventAvro scenarioAvro = ScenarioAddedEventAvro.newBuilder()
                    .setName(scenarioAdded.getName())
                    .setConditions(conditions)
                    .setActions(actions)
                    .build();
            avro.setPayload(scenarioAvro);

        } else if (event instanceof ScenarioRemovedEvent scenarioRemoved) {
            ScenarioRemovedEventAvro scenarioAvro = ScenarioRemovedEventAvro.newBuilder()
                    .setName(scenarioRemoved.getName())
                    .build();
            avro.setPayload(scenarioAvro);

        } else {
            throw new IllegalArgumentException("Unknown hub event type: " + event.getClass());
        }

        return avro;
    }

    private ScenarioConditionAvro convertConditionToAvro(ScenarioCondition condition) {
        ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()));

        // Обрабатываем value (может быть Integer, Boolean или null)
        if (condition.getValue() == null) {
            builder.setValue(null);
        } else if (condition.getValue() instanceof Integer) {
            builder.setValue((Integer) condition.getValue());
        } else if (condition.getValue() instanceof Boolean) {
            builder.setValue((Boolean) condition.getValue());
        } else {
            throw new IllegalArgumentException("Unsupported condition value type: " + condition.getValue().getClass());
        }

        return builder.build();
    }

    private DeviceActionAvro convertActionToAvro(DeviceAction action) {
        DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(ActionTypeAvro.valueOf(action.getType().name()));

        // value может быть null
        if (action.getValue() != null) {
            builder.setValue(action.getValue());
        } else {
            builder.setValue(null);
        }

        return builder.build();
    }
}