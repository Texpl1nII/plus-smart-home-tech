package ru.yandex.practicum.telemetry.collector.kafka;

import jakarta.annotation.PreDestroy;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.Properties;

@Configuration
public class KafkaProducerConfig {

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServer;

    @Bean
    public KafkaClientProducer getProducer() {
        return new KafkaClientProducer() {
            private Producer<String, SpecificRecordBase> producer;

            private void initProducer() {
                Properties config = new Properties();
                config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
                config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GeneralAvroSerializer.class);

                // Добавьте для отладки
                config.put(ProducerConfig.CLIENT_ID_CONFIG, "collector-producer");
                config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000); // 10 секунд таймаут
                config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

                System.out.println("DEBUG: Creating KafkaProducer with bootstrap servers: " + bootstrapServer);

                producer = new KafkaProducer<>(config);
                System.out.println("DEBUG: KafkaProducer created successfully");
            }

            @Override
            public Producer<String, SpecificRecordBase> getProducer() {
                if (producer == null) {
                    System.out.println("DEBUG: Initializing KafkaProducer...");
                    initProducer();
                }
                return producer;
            }

            @Override
            public void stop() {
                if (producer != null) {
                    System.out.println("DEBUG: Stopping KafkaProducer...");
                    producer.flush();
                    producer.close(Duration.ofSeconds(30));
                    System.out.println("DEBUG: KafkaProducer stopped");
                }
            }
        };
    }

    @PreDestroy
    public void onDestroy() {
        getProducer().stop();
    }
}
