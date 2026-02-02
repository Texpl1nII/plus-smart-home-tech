package ru.yandex.practicum.telemetry.analyzer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.telemetry.analyzer.processor.HubEventProcessor;
import ru.yandex.practicum.telemetry.analyzer.processor.SnapshotEventProcessor;  // ← ПРАВИЛЬНЫЙ ИМПОРТ!

@Slf4j
@SpringBootApplication
@ConfigurationPropertiesScan
public class Analyzer {
    public static void main(String[] args) {
        log.info("🚀 Starting Analyzer application...");

        ConfigurableApplicationContext context =
                SpringApplication.run(Analyzer.class, args);

        final HubEventProcessor hubEventProcessor =
                context.getBean(HubEventProcessor.class);
        SnapshotEventProcessor snapshotEventProcessor =  // ← ИЗМЕНИЛИ ТИП!
                context.getBean(SnapshotEventProcessor.class);

        Thread hubEventsThread = new Thread(hubEventProcessor);
        hubEventsThread.setName("HubEventHandlerThread");
        hubEventsThread.start();
        log.info("HubEventProcessor thread started");

        log.info("Starting SnapshotEventProcessor...");
        snapshotEventProcessor.start();  // ← Метод start() из SnapshotEventProcessor
    }
}