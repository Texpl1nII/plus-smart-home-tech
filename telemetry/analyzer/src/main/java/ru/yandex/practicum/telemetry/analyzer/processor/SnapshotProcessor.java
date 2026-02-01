package ru.yandex.practicum.telemetry.analyzer.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.telemetry.analyzer.deserializer.SensorsSnapshotDeserializer;
import ru.yandex.practicum.telemetry.analyzer.kafka.KafkaClient;
import ru.yandex.practicum.telemetry.analyzer.service.SnapshotHandler;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.List;

@Slf4j
@Component
public class SnapshotProcessor implements Runnable {

    private final KafkaClient kafkaClient;
    private final SnapshotHandler snapshotHandler;
    private final SensorsSnapshotDeserializer snapshotDeserializer;
    private Consumer<String, byte[]> snapshotConsumer;

    @Value("${analyzer.kafka.topics.snapshots-events}")
    private String snapshotsTopic;

    public SnapshotProcessor(KafkaClient kafkaClient,
                             SnapshotHandler snapshotHandler,
                             SensorsSnapshotDeserializer snapshotDeserializer) {
        this.kafkaClient = kafkaClient;
        this.snapshotHandler = snapshotHandler;
        this.snapshotDeserializer = snapshotDeserializer;
    }

    @PostConstruct
    public void init() {
        this.snapshotConsumer = kafkaClient.getSnapshotConsumer();
        log.info("SnapshotProcessor initialized with topic: {}", snapshotsTopic);
        log.info("Snapshot consumer: {}", snapshotConsumer != null ? "OK" : "NULL");
        log.info("SnapshotHandler: {}", snapshotHandler != null ? "OK" : "NULL");
    }

    @Override
    public void run() {
        log.info("▶️ SnapshotProcessor thread STARTED");
        try {
            start();
        } catch (Exception e) {
            log.error("❌ SnapshotProcessor thread crashed", e);
        }
        log.info("⏹️ SnapshotProcessor thread STOPPED");
    }

    public void start() {
        if (snapshotConsumer == null) {
            log.error("Snapshot consumer is null! Cannot start SnapshotProcessor.");
            return;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook: waking up snapshot consumer");
            snapshotConsumer.wakeup();
        }));

        try {
            snapshotConsumer.subscribe(List.of(snapshotsTopic));
            log.info("Subscribed to snapshot topic: {}", snapshotsTopic);

            int processedCount = 0;

            while (true) {
                ConsumerRecords<String, byte[]> records = snapshotConsumer.poll(Duration.ofMillis(1000));

                if (!records.isEmpty()) {
                    log.info("📥 Received {} snapshot records", records.count());

                    for (ConsumerRecord<String, byte[]> record : records) {
                        try {
                            log.debug("Processing snapshot from partition {}, offset {}",
                                    record.partition(), record.offset());

                            SensorsSnapshotAvro snapshot = snapshotDeserializer.deserialize(
                                    record.topic(), record.value());

                            if (snapshot != null) {
                                log.info("Processing snapshot for hub: {} ({} sensors)",
                                        snapshot.getHubId(),
                                        snapshot.getSensorsState() != null ?
                                                snapshot.getSensorsState().size() : 0);

                                snapshotHandler.handle(snapshot);
                                processedCount++;
                                log.info("✅ Processed snapshot for hub: {}", snapshot.getHubId());
                            } else {
                                log.warn("⚠️ Deserialized snapshot is null");
                            }
                        } catch (Exception e) {
                            log.error("❌ Error processing snapshot: {}", e.getMessage(), e);
                        }
                    }

                    snapshotConsumer.commitAsync();
                    log.debug("Committed offsets for snapshot consumer");
                }
            }
        } catch (WakeupException e) {
            log.info("SnapshotProcessor received wakeup signal, shutting down...");
        } catch (Exception e) {
            log.error("Unexpected error in SnapshotProcessor", e);
        } finally {
            try {
                log.info("Committing final offsets for snapshot consumer");
                snapshotConsumer.commitSync();
            } catch (Exception e) {
                log.error("Error during final commit", e);
            } finally {
                snapshotConsumer.close();
                log.info("SnapshotProcessor consumer closed");
            }
        }
    }
}