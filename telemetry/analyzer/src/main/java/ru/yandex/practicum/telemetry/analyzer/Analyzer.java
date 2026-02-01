package ru.yandex.practicum.telemetry.analyzer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.telemetry.analyzer.processor.HubEventProcessor;
import ru.yandex.practicum.telemetry.analyzer.processor.SnapshotProcessor;

@Slf4j
@SpringBootApplication
@ConfigurationPropertiesScan
public class Analyzer {
    public static void main(String[] args) {
        log.info("🚀 Starting Analyzer application...");

        ConfigurableApplicationContext context =
                SpringApplication.run(Analyzer.class, args);

        log.info("✓ Spring context initialized");

        try {
            // Получаем бины процессоров
            final HubEventProcessor hubEventProcessor =
                    context.getBean(HubEventProcessor.class);
            final SnapshotProcessor snapshotProcessor =
                    context.getBean(SnapshotProcessor.class);

            log.info("✓ HubEventProcessor bean: {}", hubEventProcessor != null);
            log.info("✓ SnapshotProcessor bean: {}", snapshotProcessor != null);

            // Запускаем обработчик событий от хабов в отдельном потоке
            Thread hubEventsThread = new Thread(hubEventProcessor);
            hubEventsThread.setName("HubEventHandlerThread");
            hubEventsThread.start();
            log.info("✓ HubEventProcessor started in thread: {}", hubEventsThread.getName());

            // Запускаем обработчик снапшотов в отдельном потоке
            Thread snapshotThread = new Thread(snapshotProcessor);
            snapshotThread.setName("SnapshotProcessorThread");
            snapshotThread.start();
            log.info("✓ SnapshotProcessor started in thread: {}", snapshotThread.getName());

            log.info("✅ Analyzer application fully started and running");

            // Ждем завершения потоков
            hubEventsThread.join();
            snapshotThread.join();

        } catch (Exception e) {
            log.error("❌ Error starting Analyzer", e);
            System.exit(1);
        }
    }
}