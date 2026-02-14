package ru.yandex.practicum.telemetry.aggregator.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class SnapshotService {
    private final Map<String, SensorsSnapshotAvro> snapshots = new ConcurrentHashMap<>();

    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        if (event == null || event.getHubId() == null || event.getId() == null) {
            return Optional.empty();
        }

        String hubId = event.getHubId();
        String sensorId = event.getId();

        // Получаем или создаем снапшот
        SensorsSnapshotAvro snapshot = snapshots.computeIfAbsent(hubId, id ->
                SensorsSnapshotAvro.newBuilder()
                        .setHubId(hubId)
                        .setTimestamp(event.getTimestamp())
                        .setSensorsState(new HashMap<>())
                        .build()
        );

        // Копируем текущие состояния
        Map<String, SensorStateAvro> sensorStates = new HashMap<>(snapshot.getSensorsState());
        SensorStateAvro oldState = sensorStates.get(sensorId);

        Instant newTimestamp = event.getTimestamp();

        // Проверяем, нужно ли обновлять
        if (oldState != null) {
            Instant oldTimestamp = oldState.getTimestamp();

            // Если старое событие более новое или данные не изменились
            if (oldTimestamp.isAfter(newTimestamp) ||
                    oldState.getData().equals(event.getPayload())) {
                return Optional.empty();
            }
        }

        // Создаем новое состояние
        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();

        // Обновляем состояния
        sensorStates.put(sensorId, newState);

        // Создаем обновленный снапшот (копируя старый)
        SensorsSnapshotAvro updatedSnapshot = SensorsSnapshotAvro.newBuilder(snapshot)
                .setTimestamp(event.getTimestamp())
                .setSensorsState(sensorStates)
                .build();

        // Сохраняем
        snapshots.put(hubId, updatedSnapshot);

        return Optional.of(updatedSnapshot);
    }
}
