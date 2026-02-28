package ru.yandex.practicum.telemetry.analyzer.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.analyzer.kafka.KafkaClient;
import ru.yandex.practicum.telemetry.analyzer.service.SnapshotHandler; // ← ПРАВИЛЬНЫЙ импорт!

import java.time.Duration;
import java.util.List;

@Slf4j
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
        log.info("🚀 Starting SnapshotEventProcessor...");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered for SnapshotEventProcessor");
            snapshotConsumer.wakeup();
        }));

        try {
            snapshotConsumer.subscribe(List.of(snapshotEventsTopic));
            log.info("Subscribed to topic: {}", snapshotEventsTopic);

            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records =
                        snapshotConsumer.poll(Duration.ofMillis(1000));

                if (!records.isEmpty()) {
                    log.info("Received {} snapshot records", records.count());

                    for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                        SensorsSnapshotAvro sensorsSnapshot = record.value();
                        log.debug("Processing snapshot for hub: {}", sensorsSnapshot.getHubId());

                        try {
                            snapshotHandler.handle(sensorsSnapshot);
                        } catch (Exception e) {
                            log.error("Error handling snapshot for hub {}",
                                    sensorsSnapshot.getHubId(), e);
                        }
                    }

                    snapshotConsumer.commitAsync();
                }
            }
        } catch (WakeupException ignored) {
            log.info("WakeupException caught, shutting down...");
        } catch (Exception e) {
            log.error("❌ Error processing snapshots", e);
        } finally {
            try {
                snapshotConsumer.commitSync();
                log.info("Offsets committed synchronously");
            } finally {
                snapshotConsumer.close();
                log.info("Snapshot consumer closed");
            }
        }
    }
}