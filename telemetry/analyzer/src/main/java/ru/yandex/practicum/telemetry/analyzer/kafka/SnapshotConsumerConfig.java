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

    private String bootstrapServer;
    private String groupId;
    private boolean enableAutoCommit = false;
    private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    private String valueDeserializer = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
    private Integer timeOut = 1000;

    @Bean("snapshotKafkaConsumer")
    public KafkaConsumer<String, byte[]> snapshotKafkaConsumer() {
        Properties config = new Properties();

        // Всегда используем дефолтные значения для надежности
        String server = (bootstrapServer != null && !bootstrapServer.isEmpty())
                ? bootstrapServer
                : "localhost:9092";

        String group = (groupId != null && !groupId.isEmpty())
                ? groupId
                : "snapshot.analyzer";

        log.info("Configuring snapshot Kafka consumer: bootstrapServer={}, groupId={}",
                server, group);

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Дополнительные настройки для надежности
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        config.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        config.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        return new KafkaConsumer<>(config);
    }
}