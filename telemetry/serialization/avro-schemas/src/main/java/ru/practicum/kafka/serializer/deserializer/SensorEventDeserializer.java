package ru.practicum.kafka.serializer.deserializer;

import ru.yandex.practicum.practicum.infra.gateway.kafka.telemetry.event.SensorEventAvro;

public class SensorEventDeserializer extends BaseAvroDeserializer<SensorEventAvro> {
    public SensorEventDeserializer() {
        super(SensorEventAvro.getClassSchema());
    }
}
