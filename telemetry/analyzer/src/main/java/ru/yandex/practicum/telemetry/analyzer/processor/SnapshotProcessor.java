package ru.yandex.practicum.telemetry.analyzer.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.telemetry.analyzer.deserializer.SensorsSnapshotDeserializer;
import ru.yandex.practicum.telemetry.analyzer.kafka.KafkaClient;
import ru.yandex.practicum.telemetry.analyzer.service.SnapshotHandler;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
public class SnapshotProcessor {

    private final KafkaClient kafkaClient;
    private final SnapshotHandler snapshotHandler;
    private final SensorsSnapshotDeserializer snapshotDeserializer;

    @Value("${analyzer.kafka.topics.snapshots-events}")
    private String snapshotsTopic;

    public SnapshotProcessor(KafkaClient kafkaClient,
                             SnapshotHandler snapshotHandler,
                             SensorsSnapshotDeserializer snapshotDeserializer) {
        this.kafkaClient = kafkaClient;
        this.snapshotHandler = snapshotHandler;
        this.snapshotDeserializer = snapshotDeserializer;
    }

    public void start() {
        var consumer = kafkaClient.getSnapshotConsumer();

        // Добавляем shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            consumer.subscribe(List.of(snapshotsTopic));
            log.info("SnapshotProcessor started, subscribed to topic: {}", snapshotsTopic);

            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, byte[]> record : records) {
                    try {
                        // Ручная десериализация
                        SensorsSnapshotAvro snapshot = snapshotDeserializer.deserialize(
                                record.topic(), record.value());
                        snapshotHandler.handle(snapshot);
                        log.debug("Processed snapshot for hub: {}", snapshot.getHubId());
                    } catch (Exception e) {
                        log.error("Error processing snapshot: {}", e.getMessage(), e);
                    }
                }

                consumer.commitAsync();
            }
        } catch (WakeupException e) {
            log.info("SnapshotProcessor received wakeup signal");
        } catch (Exception e) {
            log.error("Error in SnapshotProcessor", e);
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
                log.info("SnapshotProcessor stopped");
            }
        }
    }
}
