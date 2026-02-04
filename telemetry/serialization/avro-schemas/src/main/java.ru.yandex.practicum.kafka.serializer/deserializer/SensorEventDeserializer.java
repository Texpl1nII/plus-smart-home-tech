package ru.yandex.practicum.kafka.serializer.deserializer;

import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

public class SensorEventDeserializer extends ru.yandex.practicum.kafka.serializer.deserializer.BaseAvroDeserializer<SensorEventAvro> {
    public SensorEventDeserializer() {
        super(SensorEventAvro.getClassSchema());
    }
}
