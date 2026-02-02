package ru.yandex.practicum.telemetry.analyzer.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;  // ← ДОБАВЬТЕ!
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.analyzer.handler.HubEventHandler;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

@Slf4j  // ← ДОБАВЬТЕ!
@Component
@RequiredArgsConstructor
public class DeviceRemovedEventHandler implements HubEventHandler {

    private final SensorRepository repository;

    @Override
    public String getEventType() {
        return DeviceRemovedEventAvro.class.getSimpleName();
    }

    @Override
    public void handle(HubEventAvro event) {
        log.info("🔴 DEVICE_REMOVED EVENT START");

        DeviceRemovedEventAvro deviceRemovedEventAvro = (DeviceRemovedEventAvro) event.getPayload();
        log.info("Removing sensor: id={}, hub={}",
                deviceRemovedEventAvro.getId(),
                event.getHubId());

        repository.deleteByIdAndHubId(deviceRemovedEventAvro.getId(), event.getHubId());

        log.info("✅ Sensor removed");
        log.info("🔴 DEVICE_REMOVED EVENT END");
    }
}
