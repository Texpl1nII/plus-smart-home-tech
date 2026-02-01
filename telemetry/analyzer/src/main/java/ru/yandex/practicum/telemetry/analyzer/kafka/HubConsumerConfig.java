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
@ConfigurationProperties(prefix = "analyzer.kafka.consumer.hub")
public class HubConsumerConfig {

    // Поля ДОЛЖНЫ совпадать с именами в YAML (без дефисов, camelCase)
    private String bootstrapServer = "localhost:9092";  // ← дефолтное значение

    private String groupId = "events.analyzer";  // ← дефолтное значение

    private String autoOffsetReset = "earliest";

    private boolean enableAutoCommit = false;

    private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

    private String valueDeserializer = "org.apache.kafka.common.serialization.ByteArrayDeserializer";

    @Bean("hubKafkaConsumer")
    public KafkaConsumer<String, byte[]> hubKafkaConsumer() {
        Properties config = new Properties();

        log.info("Configuring hub Kafka consumer: bootstrapServer={}, groupId={}",
                bootstrapServer, groupId);

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
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
