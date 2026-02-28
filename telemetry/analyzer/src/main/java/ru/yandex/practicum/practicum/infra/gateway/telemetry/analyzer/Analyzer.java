package ru.yandex.practicum.practicum.infra.gateway.telemetry.analyzer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.practicum.infra.gateway.telemetry.analyzer.processor.HubEventProcessor;
import ru.yandex.practicum.practicum.infra.gateway.telemetry.analyzer.processor.SnapshotEventProcessor;

@Slf4j
@SpringBootApplication
@ConfigurationPropertiesScan
public class Analyzer {
    public static void main(String[] args) {
        log.info("🚀 Starting Analyzer application...");

        ConfigurableApplicationContext context =
                SpringApplication.run(Analyzer.class, args);

        try {
            final HubEventProcessor hubEventProcessor =
                    context.getBean(HubEventProcessor.class);

            final SnapshotEventProcessor snapshotEventProcessor =
                    context.getBean(SnapshotEventProcessor.class);

            // Запускаем оба процессора в отдельных потоках
            Thread hubEventsThread = new Thread(hubEventProcessor, "HubEventProcessor");
            hubEventsThread.start();
            log.info("HubEventProcessor thread started");

            Thread snapshotThread = new Thread(snapshotEventProcessor::start, "SnapshotEventProcessor");
            snapshotThread.start();
            log.info("SnapshotEventProcessor thread started");

        } catch (Exception e) {
            log.error("❌ Error starting processors", e);
        }
    }
}