package ru.yandex.practicum.telemetry.analyzer.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.analyzer.handler.HubEventHandler;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceRemovedEventHandler implements HubEventHandler {

    private final SensorRepository sensorRepository;

    @Override
    public String getEventType() {
        return DeviceRemovedEventAvro.class.getSimpleName();
    }

    @Override
    public void handle(HubEventAvro event) {
        DeviceRemovedEventAvro deviceEvent = (DeviceRemovedEventAvro) event.getPayload();

        sensorRepository.deleteByIdAndHubId(deviceEvent.getId(), event.getHubId());
        log.info("Device removed: {} from hub: {}", deviceEvent.getId(), event.getHubId());
    }
}
