package ru.yandex.practicum.telemetry.analyzer.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;  // ← ДОБАВЬТЕ ЭТО!
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.analyzer.handler.HubEventHandler;
import ru.yandex.practicum.telemetry.analyzer.model.Sensor;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

@Slf4j  // ← ДОБАВЬТЕ ЭТО!
@Component
@RequiredArgsConstructor
public class DeviceAddedEventHandler implements HubEventHandler {

    private final SensorRepository repository;

    @Override
    public String getEventType() {
        return DeviceAddedEventAvro.class.getSimpleName();
    }

    @Override
    public void handle(HubEventAvro event) {
        log.info("🟢 DEVICE_ADDED EVENT START");

        DeviceAddedEventAvro deviceAddedEventAvro = (DeviceAddedEventAvro) event.getPayload();
        log.info("Saving sensor: id={}, hub={}, type={}",
                deviceAddedEventAvro.getId(),
                event.getHubId(),
                deviceAddedEventAvro.getType());

        Sensor sensor = Sensor.builder()
                .id(deviceAddedEventAvro.getId())
                .hubId(event.getHubId())
                .build();

        Sensor saved = repository.save(sensor);
        log.info("✅ Sensor saved: id={}, hub={}", saved.getId(), saved.getHubId());

        log.info("🟢 DEVICE_ADDED EVENT END");
    }
}