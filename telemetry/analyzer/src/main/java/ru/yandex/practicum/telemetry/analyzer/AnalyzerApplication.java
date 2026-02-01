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
        log.info("Starting AnalyzerApplication...");

        ConfigurableApplicationContext context = SpringApplication.run(AnalyzerApplication.class, args);
        log.info("Spring context started successfully");

        // Получаем процессоры и клиент Kafka
        HubEventProcessor hubProcessor = context.getBean(HubEventProcessor.class);
        SnapshotProcessor snapshotProcessor = context.getBean(SnapshotProcessor.class);
        KafkaClient kafkaClient = context.getBean(KafkaClient.class);

        // Запускаем обработку hub events в отдельном потоке
        Thread hubThread = new Thread(hubProcessor);
        hubThread.setName("hub-event-processor");
        hubThread.start();
        log.info("Hub event processor started in separate thread");

        // Запускаем обработку snapshots в отдельном потоке
        Thread snapshotThread = new Thread(snapshotProcessor::start); // <- ИСПРАВЛЕНО
        snapshotThread.setName("snapshot-processor");
        snapshotThread.start();
        log.info("Snapshot processor started in separate thread");

        // Добавляем graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down AnalyzerApplication...");

            // Прерываем потоки
            hubThread.interrupt();
            snapshotThread.interrupt();

            // Закрываем Kafka клиент
            try {
                kafkaClient.close();
            } catch (Exception e) {
                log.warn("Error closing Kafka client: {}", e.getMessage());
            }

            // Закрываем контекст Spring
            context.close();
        }));

        log.info("All processors started successfully");
    }
}