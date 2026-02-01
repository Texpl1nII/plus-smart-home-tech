package ru.yandex.practicum.telemetry.analyzer.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public interface KafkaClient extends AutoCloseable {
    KafkaConsumer<String, byte[]> getHubConsumer();
    KafkaConsumer<String, byte[]> getSnapshotConsumer();
    void close();
}