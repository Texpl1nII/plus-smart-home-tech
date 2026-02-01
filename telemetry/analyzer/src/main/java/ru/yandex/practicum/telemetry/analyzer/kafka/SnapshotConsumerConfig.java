package ru.yandex.practicum.telemetry.analyzer.kafka;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Slf4j
@Getter
@Setter
@Configuration
@ConfigurationProperties("analyzer.kafka.consumer.snapshot")
public class SnapshotConsumerConfig {

    private String bootstrapServers;
    private String groupId;
    private String autoOffsetReset;
    private boolean enableAutoCommit;
    private String keyDeserializer;
    private String valueDeserializer;

    @Bean("snapshotKafkaConsumer")
    public KafkaConsumer<String, byte[]> snapshotKafkaConsumer() {
        Properties config = new Properties();

        // Проверка обязательных полей
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            throw new IllegalStateException("bootstrapServers is not configured for snapshot consumer");
        }
        if (groupId == null || groupId.isEmpty()) {
            throw new IllegalStateException("groupId is not configured for snapshot consumer");
        }

        log.info("Configuring snapshot Kafka consumer: bootstrapServers={}, groupId={}",
                bootstrapServers, groupId);

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // Устанавливаем значение по умолчанию если autoOffsetReset не задан
        if (autoOffsetReset != null && !autoOffsetReset.isEmpty()) {
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        } else {
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }

        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        // Дополнительные настройки
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);

        return new KafkaConsumer<>(config);
    }
}