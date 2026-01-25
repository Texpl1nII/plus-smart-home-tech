package ru.yandex.practicum.telemetry.collector.controller;

import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.telemetry.collector.dto.hub.*;
import ru.yandex.practicum.telemetry.collector.dto.hub.HubEvent;
import ru.yandex.practicum.telemetry.collector.dto.sensor.*;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.collector.service.hub.HubEventHandler;
import ru.yandex.practicum.telemetry.collector.service.sensor.SensorEventHandler;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@GrpcService
public class GrpcEventController extends CollectorControllerGrpc.CollectorControllerImplBase {

    private final Map<SensorEventProto.PayloadCase, SensorEventHandler> sensorEventHandlers;
    private final Map<HubEventProto.PayloadCase, HubEventHandler> hubEventHandlers;

    @Autowired
    public GrpcEventController(
            List<SensorEventHandler> sensorEventHandlerList,
            List<HubEventHandler> hubEventHandlerList) {

        this.sensorEventHandlers = sensorEventHandlerList.stream()
                .collect(Collectors.toMap(
                        this::getSensorPayloadCase,
                        Function.identity(),
                        (existing, replacement) -> {
                            throw new IllegalStateException(
                                    "Найдено несколько обработчиков для типа события: " +
                                            getSensorPayloadCase(existing)
                            );
                        }
                ));

        this.hubEventHandlers = hubEventHandlerList.stream()
                .collect(Collectors.toMap(
                        this::getHubPayloadCase,
                        Function.identity(),
                        (existing, replacement) -> {
                            throw new IllegalStateException(
                                    "Найдено несколько обработчиков для типа события: " +
                                            getHubPayloadCase(existing)
                            );
                        }
                ));

        log.info("GrpcEventController создан. Sensor handlers: {}, Hub handlers: {}",
                sensorEventHandlerList.size(), hubEventHandlerList.size());
    }

    private SensorEventProto.PayloadCase getSensorPayloadCase(SensorEventHandler handler) {
        SensorEventType type = handler.getMessageType();
        return mapSensorEventTypeToPayloadCase(type);
    }

    private HubEventProto.PayloadCase getHubPayloadCase(HubEventHandler handler) {
        HubEventType type = handler.getMessageType();
        return mapHubEventTypeToPayloadCase(type);
    }

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.info("Получен gRPC запрос от сенсора: sensorId={}, hubId={}, type={}",
                    request.getId(), request.getHubId(), request.getPayloadCase());

            SensorEventProto.PayloadCase payloadCase = request.getPayloadCase();

            if (sensorEventHandlers.containsKey(payloadCase)) {
                SensorEvent dto = convertToSensorEventDto(request);
                sensorEventHandlers.get(payloadCase).handle(dto);
            } else {
                throw new IllegalArgumentException("Не найден обработчик для события " + payloadCase);
            }

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Ошибка обработки gRPC запроса от сенсора", e);
            responseObserver.onError(new StatusRuntimeException(Status.fromThrowable(e)));
        }
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.info("Получен gRPC запрос от хаба: hubId={}, type={}",
                    request.getHubId(), request.getPayloadCase());

            HubEventProto.PayloadCase payloadCase = request.getPayloadCase();

            if (hubEventHandlers.containsKey(payloadCase)) {
                HubEvent dto = convertToHubEventDto(request);
                hubEventHandlers.get(payloadCase).handle(dto);
            } else {
                // Для SCENARIO событий может не быть обработчиков - это нормально
                log.info("Обработчик для события {} не найден, создаем DTO", payloadCase);
                HubEvent dto = convertToHubEventDto(request);
                log.info("Создан DTO для события: {}", dto);
            }

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Ошибка обработки gRPC запроса от хаба", e);
            responseObserver.onError(new StatusRuntimeException(Status.fromThrowable(e)));
        }
    }

    private SensorEvent convertToSensorEventDto(SensorEventProto proto) {
        Instant timestamp = Instant.ofEpochSecond(
                proto.getTimestamp().getSeconds(),
                proto.getTimestamp().getNanos()
        );

        SensorEventProto.PayloadCase payloadCase = proto.getPayloadCase();
        SensorEvent event = createSensorEventByType(payloadCase);

        event.setId(proto.getId());
        event.setHubId(proto.getHubId());
        event.setTimestamp(timestamp);

        fillSensorEventData(event, proto);

        return event;
    }

    private SensorEvent createSensorEventByType(SensorEventProto.PayloadCase payloadCase) {
        switch (payloadCase) {
            case MOTION_SENSOR:
                return new MotionSensorEvent();
            case TEMPERATURE_SENSOR:
                return new TemperatureSensorEvent();
            case LIGHT_SENSOR:
                return new LightSensorEvent();
            case CLIMATE_SENSOR:
                return new ClimateSensorEvent();
            case SWITCH_SENSOR:
                return new SwitchSensorEvent();
            default:
                throw new IllegalArgumentException("Неизвестный тип сенсора: " + payloadCase);
        }
    }

    private void fillSensorEventData(SensorEvent event, SensorEventProto proto) {
        SensorEventProto.PayloadCase payloadCase = proto.getPayloadCase();

        switch (payloadCase) {
            case MOTION_SENSOR:
                MotionSensorProto motionProto = proto.getMotionSensor();
                MotionSensorEvent motionEvent = (MotionSensorEvent) event;
                motionEvent.setLinkQuality(motionProto.getLinkQuality());
                motionEvent.setMotion(motionProto.getMotion());
                motionEvent.setVoltage(motionProto.getVoltage());
                break;

            case TEMPERATURE_SENSOR:
                TemperatureSensorProto tempProto = proto.getTemperatureSensor();
                TemperatureSensorEvent tempEvent = (TemperatureSensorEvent) event;
                tempEvent.setTemperatureC(tempProto.getTemperatureC());
                tempEvent.setTemperatureF(tempProto.getTemperatureF());
                break;

            case LIGHT_SENSOR:
                LightSensorProto lightProto = proto.getLightSensor();
                LightSensorEvent lightEvent = (LightSensorEvent) event;
                lightEvent.setLinkQuality(lightProto.getLinkQuality());
                lightEvent.setLuminosity(lightProto.getLuminosity());
                break;

            case CLIMATE_SENSOR:
                ClimateSensorProto climateProto = proto.getClimateSensor();
                ClimateSensorEvent climateEvent = (ClimateSensorEvent) event;
                climateEvent.setTemperatureC(climateProto.getTemperatureC());
                climateEvent.setHumidity(climateProto.getHumidity());
                climateEvent.setCo2Level(climateProto.getCo2Level());
                break;

            case SWITCH_SENSOR:
                SwitchSensorProto switchProto = proto.getSwitchSensor();
                SwitchSensorEvent switchEvent = (SwitchSensorEvent) event;
                switchEvent.setState(switchProto.getState());
                break;

            default:
                throw new IllegalArgumentException("Неизвестный тип сенсора: " + payloadCase);
        }
    }

    private HubEvent convertToHubEventDto(HubEventProto proto) {
        Instant timestamp = Instant.ofEpochSecond(
                proto.getTimestamp().getSeconds(),
                proto.getTimestamp().getNanos()
        );

        HubEventProto.PayloadCase payloadCase = proto.getPayloadCase();
        HubEvent event = createHubEventByType(payloadCase);

        event.setHubId(proto.getHubId());
        event.setTimestamp(timestamp);

        fillHubEventData(event, proto);

        return event;
    }

    private HubEvent createHubEventByType(HubEventProto.PayloadCase payloadCase) {
        switch (payloadCase) {
            case DEVICE_ADDED:
                return new DeviceAddedEvent();
            case DEVICE_REMOVED:
                return new DeviceRemovedEvent();
            case SCENARIO_ADDED:
                return new ScenarioAddedEvent();
            case SCENARIO_REMOVED:
                return new ScenarioRemovedEvent();
            default:
                throw new IllegalArgumentException("Неизвестный тип события хаба: " + payloadCase);
        }
    }

    private void fillHubEventData(HubEvent event, HubEventProto proto) {
        HubEventProto.PayloadCase payloadCase = proto.getPayloadCase();

        switch (payloadCase) {
            case DEVICE_ADDED:
                DeviceAddedEventProto deviceAddedProto = proto.getDeviceAdded();
                DeviceAddedEvent deviceAddedEvent = (DeviceAddedEvent) event;
                deviceAddedEvent.setId(deviceAddedProto.getId());
                deviceAddedEvent.setDeviceType(mapDeviceTypeProto(deviceAddedProto.getType()));
                break;

            case DEVICE_REMOVED:
                DeviceRemovedEventProto deviceRemovedProto = proto.getDeviceRemoved();
                DeviceRemovedEvent deviceRemovedEvent = (DeviceRemovedEvent) event;
                deviceRemovedEvent.setId(deviceRemovedProto.getId());
                break;

            case SCENARIO_ADDED:
                ScenarioAddedEventProto scenarioAddedProto = proto.getScenarioAdded();
                ScenarioAddedEvent scenarioAddedEvent = (ScenarioAddedEvent) event;
                scenarioAddedEvent.setName(scenarioAddedProto.getName());

                // Преобразуем condition и action из Protobuf в ваши DTO
                List<ScenarioCondition> conditions = new ArrayList<>();
                for (ScenarioConditionProto conditionProto : scenarioAddedProto.getConditionList()) {
                    ScenarioCondition condition = new ScenarioCondition();
                    condition.setSensorId(conditionProto.getSensorId());
                    condition.setType(mapConditionTypeProto(conditionProto.getType()));
                    condition.setOperation(mapConditionOperationProto(conditionProto.getOperation()));

                    // Заполняем значение в зависимости от типа
                    switch (conditionProto.getValueCase()) {
                        case BOOL_VALUE:
                            // В вашем DTO value - int, преобразуем bool в int
                            condition.setValue(conditionProto.getBoolValue() ? 1 : 0);
                            break;
                        case INT_VALUE:
                            condition.setValue(conditionProto.getIntValue());
                            break;
                        case VALUE_NOT_SET:
                        default:
                            condition.setValue(0); // значение по умолчанию
                            break;
                    }
                    conditions.add(condition);
                }
                scenarioAddedEvent.setConditions(conditions);

                // Преобразуем action (если в вашем DTO есть DeviceAction)
                // Если нет - оставляем пустой список
                scenarioAddedEvent.setActions(new ArrayList<>());

                log.info("Создан ScenarioAddedEvent: name={}, conditions={}",
                        scenarioAddedEvent.getName(), scenarioAddedEvent.getConditions().size());
                break;

            case SCENARIO_REMOVED:
                ScenarioRemovedEventProto scenarioRemovedProto = proto.getScenarioRemoved();
                ScenarioRemovedEvent scenarioRemovedEvent = (ScenarioRemovedEvent) event;
                scenarioRemovedEvent.setName(scenarioRemovedProto.getName());
                log.info("Создан ScenarioRemovedEvent: name={}", scenarioRemovedEvent.getName());
                break;

            default:
                throw new IllegalArgumentException("Неизвестный тип события хаба: " + payloadCase);
        }
    }

    // Вспомогательные методы для преобразования enum

    private SensorEventProto.PayloadCase mapSensorEventTypeToPayloadCase(SensorEventType type) {
        switch (type) {
            case MOTION_SENSOR_EVENT: return SensorEventProto.PayloadCase.MOTION_SENSOR;
            case TEMPERATURE_SENSOR_EVENT: return SensorEventProto.PayloadCase.TEMPERATURE_SENSOR;
            case LIGHT_SENSOR_EVENT: return SensorEventProto.PayloadCase.LIGHT_SENSOR;
            case CLIMATE_SENSOR_EVENT: return SensorEventProto.PayloadCase.CLIMATE_SENSOR;
            case SWITCH_SENSOR_EVENT: return SensorEventProto.PayloadCase.SWITCH_SENSOR;
            default: throw new IllegalArgumentException("Неизвестный SensorEventType: " + type);
        }
    }

    private HubEventProto.PayloadCase mapHubEventTypeToPayloadCase(HubEventType type) {
        switch (type) {
            case DEVICE_ADDED: return HubEventProto.PayloadCase.DEVICE_ADDED;
            case DEVICE_REMOVED: return HubEventProto.PayloadCase.DEVICE_REMOVED;
            case SCENARIO_ADDED: return HubEventProto.PayloadCase.SCENARIO_ADDED;
            case SCENARIO_REMOVED: return HubEventProto.PayloadCase.SCENARIO_REMOVED;
            default: throw new IllegalArgumentException("Неизвестный HubEventType: " + type);
        }
    }

    private DeviceType mapDeviceTypeProto(DeviceTypeProto protoType) {
        switch (protoType) {
            case MOTION_SENSOR: return DeviceType.MOTION_SENSOR;
            case TEMPERATURE_SENSOR: return DeviceType.TEMPERATURE_SENSOR;
            case LIGHT_SENSOR: return DeviceType.LIGHT_SENSOR;
            case CLIMATE_SENSOR: return DeviceType.CLIMATE_SENSOR;
            case SWITCH_SENSOR: return DeviceType.SWITCH_SENSOR;
            default: throw new IllegalArgumentException("Неизвестный DeviceTypeProto: " + protoType);
        }
    }

    private ConditionType mapConditionTypeProto(ConditionTypeProto protoType) {
        switch (protoType) {
            case MOTION: return ConditionType.MOTION;
            case LUMINOSITY: return ConditionType.LUMINOSITY;
            case SWITCH: return ConditionType.SWITCH;
            case TEMPERATURE: return ConditionType.TEMPERATURE;
            case CO2LEVEL: return ConditionType.CO2LEVEL;
            case HUMIDITY: return ConditionType.HUMIDITY;
            default: throw new IllegalArgumentException("Неизвестный ConditionTypeProto: " + protoType);
        }
    }

    private ConditionOperation mapConditionOperationProto(ConditionOperationProto protoOperation) {
        switch (protoOperation) {
            case EQUALS: return ConditionOperation.EQUALS;
            case GREATER_THAN: return ConditionOperation.GREATER_THAN;
            case LOWER_THAN: return ConditionOperation.LOWER_THAN;
            default: throw new IllegalArgumentException("Неизвестный ConditionOperationProto: " + protoOperation);
        }
    }
}
