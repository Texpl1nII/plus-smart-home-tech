package ru.yandex.practicum.telemetry.analyzer.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.telemetry.analyzer.deserializer.HubEventDeserializer;
import ru.yandex.practicum.telemetry.analyzer.handler.HubEventHandler;
import ru.yandex.practicum.telemetry.analyzer.kafka.KafkaClient;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class HubEventProcessor implements Runnable {

    private final KafkaClient kafkaClient;
    private final List<HubEventHandler> hubEventHandlers;
    private final HubEventDeserializer hubEventDeserializer;
    private KafkaConsumer<String, HubEventAvro> hubConsumer;
    private final Map<String, HubEventHandler> handlersMap = new HashMap<>();

    @Value("${analyzer.kafka.topics.hub-events}")
    private String hubEventsTopic;

    public HubEventProcessor(KafkaClient kafkaClient,
                             List<HubEventHandler> hubEventHandlers,
                             HubEventDeserializer hubEventDeserializer) {
        this.kafkaClient = kafkaClient;
        this.hubEventHandlers = hubEventHandlers;
        this.hubEventDeserializer = hubEventDeserializer;
    }

    @PostConstruct
    public void init() {
        this.hubConsumer = kafkaClient.getHubConsumer();

        // Регистрируем обработчики
        for (HubEventHandler handler : hubEventHandlers) {
            String eventType = handler.getEventType();
            handlersMap.put(eventType, handler);
            log.info("Registered handler: {} -> {}", eventType, handler.getClass().getSimpleName());
        }

        log.info("Total registered hub event handlers: {}", handlersMap.size());
        log.info("Hub consumer initialized: {}", hubConsumer != null ? "OK" : "NULL");
    }

    @Override
    public void run() {
        log.info("▶️ HubEventProcessor thread STARTED");
        try {
            start();
        } catch (Exception e) {
            log.error("❌ HubEventProcessor thread crashed", e);
        }
        log.info("⏹️ HubEventProcessor thread STOPPED");
    }

    public void start() {
        if (hubConsumer == null) {
            log.error("Hub consumer is null! Cannot start HubEventProcessor.");
            return;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook: waking up hub event consumer");
            hubConsumer.wakeup();
        }));

        try {
            hubConsumer.subscribe(List.of(hubEventsTopic));
            log.info("Subscribed to hub events topic: {}", hubEventsTopic);

            int processedCount = 0;

            while (true) {
                ConsumerRecords<String, byte[]> records = hubConsumer.poll(Duration.ofMillis(1000));

                if (!records.isEmpty()) {
                    log.info("📥 Received {} hub events", records.count());

                    for (ConsumerRecord<String, byte[]> record : records) {
                        try {
                            HubEventAvro event = hubEventDeserializer.deserialize(
                                    record.topic(), record.value());

                            if (event != null) {
                                String eventType = event.getPayload().getClass().getSimpleName();
                                log.info("Processing hub event: {} for hub: {}",
                                        eventType, event.getHubId());

                                HubEventHandler handler = handlersMap.get(eventType);
                                if (handler != null) {
                                    handler.handle(event);
                                    processedCount++;
                                    log.info("✅ Processed hub event: {} for hub: {}",
                                            eventType, event.getHubId());
                                } else {
                                    log.warn("⚠️ No handler found for event type: {}", eventType);
                                }
                            } else {
                                log.warn("⚠️ Deserialized hub event is null");
                            }
                        } catch (Exception e) {
                            log.error("❌ Error processing hub event: {}", e.getMessage(), e);
                        }
                    }

                    hubConsumer.commitAsync();
                    log.debug("Committed offsets for hub event consumer");
                }
            }
        } catch (WakeupException e) {
            log.info("HubEventProcessor received wakeup signal, shutting down...");
        } catch (Exception e) {
            log.error("Unexpected error in HubEventProcessor", e);
        } finally {
            try {
                log.info("Committing final offsets for hub event consumer");
                hubConsumer.commitSync();
            } catch (Exception e) {
                log.error("Error during final commit", e);
            } finally {
                hubConsumer.close();
                log.info("HubEventProcessor consumer closed");
            }
        }
    }
}