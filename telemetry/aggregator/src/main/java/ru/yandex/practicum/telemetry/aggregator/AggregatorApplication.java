package ru.yandex.practicum.telemetry.aggregator;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;

@Slf4j
@SpringBootApplication(exclude = {KafkaAutoConfiguration.class})
@ConfigurationPropertiesScan
public class AggregatorApplication {
    public static void main(String[] args) {
        log.info("Starting AggregatorApplication...");
        try {
            ConfigurableApplicationContext context = SpringApplication.run(AggregatorApplication.class, args);
            log.info("Spring context started successfully");

            AggregationStarter aggregator = context.getBean(AggregationStarter.class);
            log.info("Starting aggregation service...");
            aggregator.start();

        } catch (Exception e) {
            log.error("Failed to start Aggregator: {}", e.getMessage(), e);
            System.exit(1);
        }
    }
}