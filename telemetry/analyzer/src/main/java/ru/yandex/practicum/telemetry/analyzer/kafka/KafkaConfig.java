package ru.yandex.practicum.telemetry.analyzer.kafka;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

@Slf4j
@Getter
@Setter
@Configuration
@ConfigurationProperties("analyzer.kafka")
public class KafkaConfig {

    @Autowired
    private HubConsumerConfig hubConsumerConfig;

    @Autowired
    private SnapshotConsumerConfig snapshotConsumerConfig;

    @Bean
    public KafkaClient kafkaClient(
            KafkaConsumer<String, HubEventAvro> hubKafkaConsumer,
            KafkaConsumer<String, SensorsSnapshotAvro> snapshotKafkaConsumer) {
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
                    }
                } catch (Exception e) {
                    log.error("Error closing hub consumer", e);
                }

                try {
                    if (snapshotKafkaConsumer != null) {
                        snapshotKafkaConsumer.close();
                    }
                } catch (Exception e) {
                    log.error("Error closing snapshot consumer", e);
                }
            }
        };
    }
}
