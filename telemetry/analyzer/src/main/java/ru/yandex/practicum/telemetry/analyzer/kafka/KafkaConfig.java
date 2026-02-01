package ru.yandex.practicum.telemetry.analyzer.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@ConfigurationProperties("analyzer.kafka")
public class KafkaConfig {

    @Autowired
    private HubConsumerConfig hubConsumerConfig;

    @Autowired
    private SnapshotConsumerConfig snapshotConsumerConfig;

    @Bean
    public KafkaClient kafkaClient(
            org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]> hubKafkaConsumer,
            org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]> snapshotKafkaConsumer) {

        log.info("Creating KafkaClient with hub consumer: {}, snapshot consumer: {}",
                hubKafkaConsumer != null, snapshotKafkaConsumer != null);

        return new KafkaClient() {
            @Override
            public KafkaConsumer<String, byte[]> getHubConsumer() {
                return hubKafkaConsumer;
            }
            @Override
            public KafkaConsumer<String, byte[]> getSnapshotConsumer() {
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