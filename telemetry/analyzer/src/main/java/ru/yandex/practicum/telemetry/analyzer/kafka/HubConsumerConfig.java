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
// Измените путь на analyzer.kafka
@ConfigurationProperties("analyzer.kafka")
public class HubConsumerConfig {

    // Добавьте поле для bootstrap-servers из analyzer.kafka
    private String bootstrapServers;  // ← во множественном числе

    // Добавьте вложенный класс для consumer.hub
    private HubConsumer hub = new HubConsumer();

    @Getter
    @Setter
    public static class HubConsumer {
        private String groupId;
        private String autoOffsetReset = "earliest";
        private boolean enableAutoCommit;
        private String keyDeserializer;
        private String valueDeserializer;
    }

    // Если нужны topics
    private Topics topics = new Topics();

    @Getter
    @Setter
    public static class Topics {
        private String hubEvents;
        private String snapshotsEvents;
    }

    @Bean("hubKafkaConsumer")
    public KafkaConsumer<String, byte[]> hubKafkaConsumer() {
        Properties config = new Properties();

        // Проверка обязательных полей
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            throw new IllegalStateException("bootstrapServers is not configured for hub consumer");
        }
        if (hub.getGroupId() == null || hub.getGroupId().isEmpty()) {
            throw new IllegalStateException("groupId is not configured for hub consumer");
        }

        log.info("Configuring hub Kafka consumer: bootstrapServers={}, groupId={}",
                bootstrapServers, hub.getGroupId());

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, hub.getGroupId());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, hub.getAutoOffsetReset());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, hub.isEnableAutoCommit());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, hub.getKeyDeserializer());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, hub.getValueDeserializer());

        // Дополнительные настройки
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);

        return new KafkaConsumer<>(config);
    }
}
