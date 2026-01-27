package ru.yandex.practicum.telemetry.aggregator.kafka;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Slf4j
@Getter
@Setter
@Configuration
@ConfigurationProperties("aggregator.kafka")
public class KafkaConfig {

    private String bootstrapServers;

    @Autowired
    private AggregatorProducerConfig producerConfig;

    @Autowired
    private AggregatorConsumerConfig consumerConfig;

    @Bean
    public KafkaClient kafkaClient(
            KafkaProducer<String, SpecificRecordBase> kafkaProducer,
            KafkaConsumer<String, SpecificRecordBase> kafkaConsumer) {

        return new KafkaClient() {
            @Override
            public Producer<String, SpecificRecordBase> getProducer() {
                return kafkaProducer;
            }

            @Override
            public Consumer<String, SpecificRecordBase> getConsumer() {
                return kafkaConsumer;
            }

            @Override
            public void close() {
                try {
                    if (kafkaProducer != null) {
                        kafkaProducer.flush();
                        kafkaProducer.close(Duration.ofSeconds(10));
                        log.info("Producer closed");
                    }
                } catch (Exception e) {
                    log.warn("Error closing producer: {}", e.getMessage());
                }

                try {
                    if (kafkaConsumer != null) {
                        kafkaConsumer.commitSync();
                        kafkaConsumer.close();
                        log.info("Consumer closed");
                    }
                } catch (Exception e) {
                    log.warn("Error closing consumer: {}", e.getMessage());
                }
            }
        };
    }
}
