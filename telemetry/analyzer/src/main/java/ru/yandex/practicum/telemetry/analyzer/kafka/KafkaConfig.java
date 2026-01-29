package ru.yandex.practicum.telemetry.analyzer.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

@Slf4j
@Configuration
public class KafkaConfig {

    @Autowired
    private KafkaConsumer<String, HubEventAvro> hubKafkaConsumer;

    @Autowired
    private KafkaConsumer<String, SensorsSnapshotAvro> snapshotKafkaConsumer;

    @Bean
    public KafkaClient kafkaClient() {
        return new KafkaClient() {

            @Override
            public KafkaConsumer<String, HubEventAvro> getHubConsumer() {
                return hubKafkaConsumer;
            }

            @Override
            public KafkaConsumer<String, SensorsSnapshotAvro> getSnapshotConsumer() {
                return snapshotKafkaConsumer;
            }

            @Override
            public void close() {
                try {
                    if (hubKafkaConsumer != null) {
                        hubKafkaConsumer.close();
                        log.info("Hub consumer closed");
                    }
                } catch (Exception e) {
                    log.warn("Error closing hub consumer: {}", e.getMessage());
                }

                try {
                    if (snapshotKafkaConsumer != null) {
                        snapshotKafkaConsumer.close();
                        log.info("Snapshot consumer closed");
                    }
                } catch (Exception e) {
                    log.warn("Error closing snapshot consumer: {}", e.getMessage());
                }
            }
        };
    }
}
