package ru.yandex.practicum.telemetry.analyzer.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.analyzer.entity.Sensor;
import ru.yandex.practicum.telemetry.analyzer.handler.HubEventHandler;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeviceAddedEventHandler implements HubEventHandler {

    private final SensorRepository sensorRepository;

    @Override
    public String getEventType() {
        return DeviceAddedEventAvro.class.getSimpleName();
    }

    @Override
    public void handle(HubEventAvro event) {
        DeviceAddedEventAvro deviceEvent = (DeviceAddedEventAvro) event.getPayload();

        Sensor sensor = Sensor.builder()
                .id(deviceEvent.getId())
                .hubId(event.getHubId())
                .build();

        sensorRepository.save(sensor);
        log.info("Device added: {} for hub: {}", deviceEvent.getId(), event.getHubId());
    }
}
