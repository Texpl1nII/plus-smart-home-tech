package ru.yandex.practicum.kafka.serializer.deserializer;

import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

public class SensorsSnapshotDeserializer extends ru.yandex.practicum.kafka.serializer.deserializer.BaseAvroDeserializer<SensorsSnapshotAvro> {
    public SensorsSnapshotDeserializer() {
        super(SensorsSnapshotAvro.getClassSchema());
    }
}
