package ru.yandex.practicum.telemetry.analyzer.processor;


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.analyzer.kafka.KafkaClient;
import ru.yandex.practicum.telemetry.analyzer.service.SnapshotHandler;

import java.time.Duration;
import java.util.List;

@Component
public class SnapshotEventProcessor {

    private final SnapshotHandler snapshotHandler;
    private final Consumer<String, SensorsSnapshotAvro> snapshotConsumer;

    @Value("${analyzer.kafka.topics.snapshots-events}")
    private String snapshotEventsTopic;

    public SnapshotEventProcessor(SnapshotHandler snapshotHandler, KafkaClient kafkaClient) {
        this.snapshotHandler = snapshotHandler;
        this.snapshotConsumer = kafkaClient.getSnapshotConsumer();
    }

    public void start() {
        Runtime.getRuntime().addShutdownHook(new Thread(snapshotConsumer::wakeup));
        try {
            snapshotConsumer.subscribe(List.of(snapshotEventsTopic));
            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = snapshotConsumer.poll(Duration.ofMillis(1000));
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                        SensorsSnapshotAvro sensorsSnapshot = record.value();
                        snapshotHandler.handle(sensorsSnapshot);

                    }
                    snapshotConsumer.commitAsync();
                }
            }
        } catch (WakeupException ignored) {

        } catch (Exception e) {
        } finally {
            try {
                snapshotConsumer.commitSync();
            } finally {
                snapshotConsumer.close();
            }
        }
    }
}