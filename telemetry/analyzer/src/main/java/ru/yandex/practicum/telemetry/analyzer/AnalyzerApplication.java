package ru.yandex.practicum.telemetry.analyzer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.telemetry.analyzer.processor.HubEventProcessor;
import ru.yandex.practicum.telemetry.analyzer.processor.SnapshotProcessor;

@Slf4j
@SpringBootApplication(exclude = {KafkaAutoConfiguration.class})
@ConfigurationPropertiesScan
public class AnalyzerApplication {
    public static void main(String[] args) {
        log.info("Starting AnalyzerApplication...");

        ConfigurableApplicationContext context = SpringApplication.run(AnalyzerApplication.class, args);
        log.info("Spring context started successfully");

        // Получаем процессоры
        HubEventProcessor hubProcessor = context.getBean(HubEventProcessor.class);
        SnapshotProcessor snapshotProcessor = context.getBean(SnapshotProcessor.class);

        // Запускаем обработку hub events в отдельном потоке
        Thread hubThread = new Thread(hubProcessor);
        hubThread.setName("hub-event-processor");
        hubThread.start();
        log.info("Hub event processor started in separate thread");

        // Запускаем обработку snapshots в основном потоке
        log.info("Starting snapshot processor...");
        snapshotProcessor.start();
    }
}
