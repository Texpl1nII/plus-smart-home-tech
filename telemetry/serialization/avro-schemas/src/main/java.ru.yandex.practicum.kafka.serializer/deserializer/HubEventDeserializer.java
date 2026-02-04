package ru.yandex.practicum.kafka.serializer.deserializer;

import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

public class HubEventDeserializer extends ru.yandex.practicum.kafka.serializer.deserializer.BaseAvroDeserializer<HubEventAvro> {
    public HubEventDeserializer() {
        super(HubEventAvro.getClassSchema());
    }
}
