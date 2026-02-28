package ru.yandex.practicum.practicum.infra.gateway.telemetry.collector.kafka;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;

public interface KafkaClientProducer {
    Producer<String, SpecificRecordBase> getProducer();
    void stop();
}
