package ru.yandex.practicum.telemetry.aggregator.kafka;

import lombok.Getter;
import lombok.Setter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Getter
@Setter
@Configuration
@ConfigurationProperties("aggregator.kafka.consumer")
public class AggregatorConsumerConfig {

    private String bootstrapServers;
    private String groupId;
    private String autoOffsetReset;
    private boolean enableAutoCommit;
    private String keyDeserializer;
    private String valueDeserializer;

    @Bean
    public KafkaConsumer<String, SpecificRecordBase> kafkaConsumer() {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);

        return new KafkaConsumer<>(config);
    }
}
