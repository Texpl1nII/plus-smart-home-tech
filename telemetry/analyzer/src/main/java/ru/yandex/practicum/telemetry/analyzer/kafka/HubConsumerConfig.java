package ru.yandex.practicum.telemetry.analyzer.kafka;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.util.Properties;

@Getter
@Setter
@Configuration
@ConfigurationProperties("analyzer.kafka.consumer.hub")
public class HubConsumerConfig {

    private String bootstrapServer;
    private String groupId;
    private String keyDeserializer;
    private String valueDeserializer;
    private Boolean enableAutoCommit;
    private Integer timeOut;

    @Bean("hubKafkaConsumer")
    public KafkaConsumer<String, HubEventAvro> hubKafkaConsumer() {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(config);
    }
}