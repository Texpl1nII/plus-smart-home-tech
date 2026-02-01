package ru.yandex.practicum.telemetry.analyzer.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

public interface KafkaClient extends AutoCloseable{
    KafkaConsumer<String, HubEventAvro> getHubConsumer();
    KafkaConsumer<String, SensorsSnapshotAvro> getSnapshotConsumer();
    void close();
}