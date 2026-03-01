package ru.yandex.practicum.telemetry.aggregator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.aggregator.kafka.KafkaClient;
import ru.yandex.practicum.telemetry.aggregator.service.SnapshotService;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {

    @Value("${aggregator.topics.sensors-events}")
    private String sensorsEventsTopic;

    @Value("${aggregator.topics.snapshots-events}")
    private String snapshotsEventsTopic;

    private final KafkaClient kafkaClient;
    private final SnapshotService snapshotService;

    public void start() {
        Producer<String, SpecificRecordBase> producer = kafkaClient.getProducer();
        Consumer<String, SpecificRecordBase> consumer = kafkaClient.getConsumer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook - waking up consumer");
            consumer.wakeup();
        }));

        try {
            consumer.subscribe(List.of(sensorsEventsTopic));

            while (true) {
                ConsumerRecords<String, SpecificRecordBase> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, SpecificRecordBase> record : records) {
                    try {
                        SensorEventAvro event = (SensorEventAvro) record.value();
                        Optional<SensorsSnapshotAvro> updatedSnapshot = snapshotService.updateState(event);

                        if (updatedSnapshot.isPresent()) {
                            SensorsSnapshotAvro snapshot = updatedSnapshot.get();
                            producer.send(new ProducerRecord<>(
                                    snapshotsEventsTopic,
                                    snapshot.getHubId(),
                                    snapshot
                            ));
                        }
                    } catch (ClassCastException e) {
                        log.warn("Invalid message type, skipping");
                    }
                }

                consumer.commitAsync();
            }
        } catch (WakeupException e) {
            log.info("WakeupException - shutting down");
        } catch (Exception e) {
            log.error("Error in aggregation loop", e);
        } finally {
            kafkaClient.close();
            log.info("Aggregator stopped");
        }
    }
}
