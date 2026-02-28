package ru.yandex.practicum.practicum.infra.gateway.telemetry.collector.service.hub;

import ru.yandex.practicum.practicum.infra.gateway.telemetry.collector.dto.hub.HubEvent;
import ru.yandex.practicum.practicum.infra.gateway.telemetry.collector.dto.hub.HubEventType;

public interface HubEventHandler {
    HubEventType getMessageType();
    void handle(HubEvent event);
}
