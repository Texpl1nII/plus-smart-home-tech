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
@ConfigurationProperties("analyzer.kafka.consumer.hub")
public class HubConsumerConfig {

    private String bootstrapServers; // ← ИЗМЕНИТЕ на bootstrapServers (мн.ч.)
    private String groupId;
    private String autoOffsetReset = "earliest";
    private boolean enableAutoCommit;
    private String keyDeserializer;
    private String valueDeserializer;

    @Bean("hubKafkaConsumer")
    public KafkaConsumer<String, byte[]> hubKafkaConsumer() {
        Properties config = new Properties();

        // Используйте ИЛИ переменную окружения ИЛИ значение из конфигурации
        String servers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (servers == null || servers.isEmpty()) {
            servers = bootstrapServers; // из конфигурации
        }

        if (servers == null || servers.isEmpty()) {
            throw new IllegalStateException("Kafka bootstrap servers not configured!");
        }

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);  // ← используйте bootstrapServer
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);

        // Дополнительные настройки
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);

        return new KafkaConsumer<>(config);
    }
}
