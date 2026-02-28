package ru.yandex.practicum.practicum.infra.gateway.telemetry.analyzer.handler;

import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

public interface HubEventHandler {
    String getEventType();
    void handle(HubEventAvro event);
}