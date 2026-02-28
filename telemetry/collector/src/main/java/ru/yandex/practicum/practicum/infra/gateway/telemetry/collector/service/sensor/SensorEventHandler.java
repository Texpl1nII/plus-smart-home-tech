package ru.yandex.practicum.practicum.infra.gateway.telemetry.collector.service.sensor;

import ru.yandex.practicum.practicum.infra.gateway.telemetry.collector.dto.sensor.SensorEvent;
import ru.yandex.practicum.practicum.infra.gateway.telemetry.collector.dto.sensor.SensorEventType;

public interface SensorEventHandler {
    SensorEventType getMessageType();
    void handle(SensorEvent event);
}
