package ru.yandex.practicum.telemetry.analyzer.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;  // ← ДОБАВЬТЕ ЭТО!
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.analyzer.handler.HubEventHandler;
import ru.yandex.practicum.telemetry.analyzer.model.Sensor;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

import java.util.Optional;

@Slf4j
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
        String sensorId = deviceAddedEventAvro.getId();
        String hubId = event.getHubId();

        log.info("Processing device added: id={}, hub={}, type={}",
                sensorId, hubId, deviceAddedEventAvro.getType());

        Optional<Sensor> existingSensor = repository.findByIdAndHubId(sensorId, hubId);

        if (existingSensor.isPresent()) {
            log.info("Sensor already exists: id={}, hub={}", sensorId, hubId);
            log.info("🟢 DEVICE_ADDED EVENT END (already exists)");
            return;
        }

        Sensor sensor = Sensor.builder()
                .id(sensorId)
                .hubId(hubId)
                .build();

        Sensor saved = repository.save(sensor);
        log.info("✅ Sensor saved: id={}, hub={}", saved.getId(), saved.getHubId());

        log.info("🟢 DEVICE_ADDED EVENT END");
    }
}