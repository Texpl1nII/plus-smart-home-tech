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
        // Используем Builder, а не конструктор!
        SensorEventAvro.Builder builder = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())  // camelCase!
                .setTimestamp(event.getTimestamp().toEpochMilli());

        if (event instanceof LightSensorEvent lightEvent) {
            LightSensorAvro lightAvro = LightSensorAvro.newBuilder()
                    .setLinkQuality(lightEvent.getLinkQuality())
                    .setLuminosity(lightEvent.getLuminosity())
                    .build();
            builder.setPayload(lightAvro);

        } else if (event instanceof TemperatureSensorEvent tempEvent) {
            TemperatureSensorAvro tempAvro = TemperatureSensorAvro.newBuilder()
                    .setTemperatureC(tempEvent.getTemperatureC())
                    .setTemperatureF(tempEvent.getTemperatureF())
                    .build();
            builder.setPayload(tempAvro);

        } else if (event instanceof ClimateSensorEvent climateEvent) {
            ClimateSensorAvro climateAvro = ClimateSensorAvro.newBuilder()
                    .setTemperatureC(climateEvent.getTemperatureC())
                    .setHumidity(climateEvent.getHumidity())
                    .setCo2Level(climateEvent.getCo2Level())
                    .build();
            builder.setPayload(climateAvro);

        } else if (event instanceof MotionSensorEvent motionEvent) {
            MotionSensorAvro motionAvro = MotionSensorAvro.newBuilder()
                    .setLinkQuality(motionEvent.getLinkQuality())
                    .setMotion(motionEvent.isMotion())
                    .setVoltage(motionEvent.getVoltage())
                    .build();
            builder.setPayload(motionAvro);

        } else if (event instanceof SwitchSensorEvent switchEvent) {
            SwitchSensorAvro switchAvro = SwitchSensorAvro.newBuilder()
                    .setState(switchEvent.isState())
                    .build();
            builder.setPayload(switchAvro);
        } else {
            throw new IllegalArgumentException("Unknown sensor event type: " + event.getClass());
        }

        return builder.build();  // Не забудь build()!
    }

    public HubEventAvro convertToAvro(HubEvent event) {
        // Используем Builder!
        HubEventAvro.Builder builder = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())  // camelCase, хотя в схеме hub_id!
                .setTimestamp(event.getTimestamp().toEpochMilli());

        if (event instanceof DeviceAddedEvent deviceAdded) {
            DeviceAddedEventAvro deviceAvro = DeviceAddedEventAvro.newBuilder()
                    .setId(deviceAdded.getId())
                    .setType(DeviceTypeAvro.valueOf(deviceAdded.getDeviceType().name()))
                    .build();
            builder.setPayload(deviceAvro);

        } else if (event instanceof DeviceRemovedEvent deviceRemoved) {
            DeviceRemovedEventAvro deviceAvro = DeviceRemovedEventAvro.newBuilder()
                    .setId(deviceRemoved.getId())
                    .build();
            builder.setPayload(deviceAvro);

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
            builder.setPayload(scenarioAvro);

        } else if (event instanceof ScenarioRemovedEvent scenarioRemoved) {
            ScenarioRemovedEventAvro scenarioAvro = ScenarioRemovedEventAvro.newBuilder()
                    .setName(scenarioRemoved.getName())
                    .build();
            builder.setPayload(scenarioAvro);

        } else {
            throw new IllegalArgumentException("Unknown hub event type: " + event.getClass());
        }

        return builder.build();  // Не забудь build()!
    }

    private ScenarioConditionAvro convertConditionToAvro(ScenarioCondition condition) {
        ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()));

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

        if (action.getValue() != null) {
            builder.setValue(action.getValue());
        } else {
            builder.setValue(null);
        }

        return builder.build();
    }
}