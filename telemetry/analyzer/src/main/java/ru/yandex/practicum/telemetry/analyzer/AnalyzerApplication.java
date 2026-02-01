package ru.yandex.practicum.telemetry.analyzer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.telemetry.analyzer.kafka.KafkaClient;
import ru.yandex.practicum.telemetry.analyzer.processor.HubEventProcessor;
import ru.yandex.practicum.telemetry.analyzer.processor.SnapshotProcessor;

@Slf4j
@SpringBootApplication
@ConfigurationPropertiesScan
public class AnalyzerApplication {
    public static void main(String[] args) {

        System.out.println("=== CI DEBUG ===");
        System.out.println("Java version: " + System.getProperty("java.version"));
        System.out.println("Working dir: " + System.getProperty("user.dir"));
        System.out.println("Kafka servers: " + System.getenv("KAFKA_BOOTSTRAP_SERVERS"));
        System.out.println("Database URL: " + System.getenv("DATABASE_URL"));
        System.out.println("=== END CI DEBUG ===");

        log.info("Starting AnalyzerApplication...");

        ConfigurableApplicationContext context = SpringApplication.run(AnalyzerApplication.class, args);
        log.info("Spring context started successfully");

        // Получаем бины
        HubEventProcessor hubProcessor = context.getBean(HubEventProcessor.class);
        SnapshotProcessor snapshotProcessor = context.getBean(SnapshotProcessor.class);
        KafkaClient kafkaClient = context.getBean(KafkaClient.class);

        log.info("Starting processors...");

        // Запускаем обработку hub events
        Thread hubThread = new Thread(() -> {
            log.info("HubEventProcessor thread started");
            try {
                hubProcessor.run();
            } catch (Exception e) {
                log.error("HubEventProcessor failed", e);
            }
        });
        hubThread.setName("hub-event-processor");
        hubThread.start();
        log.info("Hub event processor started");

        // Даем время для инициализации
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Запускаем обработку snapshots
        Thread snapshotThread = new Thread(() -> {
            log.info("SnapshotProcessor thread started");
            try {
                snapshotProcessor.start();
            } catch (Exception e) {
                log.error("SnapshotProcessor failed", e);
            }
        });
        snapshotThread.setName("snapshot-processor");
        snapshotThread.start();
        log.info("Snapshot processor started");

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        log.info("All processors started. Application is running.");

        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down AnalyzerApplication...");

            // Прерываем потоки
            hubThread.interrupt();
            snapshotThread.interrupt();

            // Закрываем ресурсы
            try {
                kafkaClient.close();
                log.info("Kafka client closed");
            } catch (Exception e) {
                log.warn("Error closing Kafka client: {}", e.getMessage());
            }

            context.close();
            log.info("Spring context closed");
        }));
    }
}