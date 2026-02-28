package ru.yandex.practicum.telemetry.analyzer.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.analyzer.handler.HubEventHandler;
import ru.yandex.practicum.telemetry.analyzer.kafka.KafkaClient;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
@Slf4j
public class HubEventProcessor implements Runnable {

    private final Consumer<String, HubEventAvro> hubConsumer;
    private final Map<Class<?>, HubEventHandler> hubEventHandlers;

    @Value("${analyzer.kafka.topics.hub-events}")
    private String hubEventsTopic;

    public HubEventProcessor(KafkaClient kafkaClient, List<HubEventHandler> hubEventHandlers) {
        this.hubConsumer = kafkaClient.getHubConsumer();
        this.hubEventHandlers = hubEventHandlers.stream()
                .collect(Collectors.toMap(
                        handler -> getPayloadClass(handler.getEventType()),
                        Function.identity()
                ));
        log.info("Registered handlers for event types: {}",
                this.hubEventHandlers.keySet().stream()
                        .map(Class::getSimpleName)
                        .collect(Collectors.toList()));
    }

    private Class<?> getPayloadClass(String eventType) {
        try {
            // Убираем "Avro" из конца, если оно уже есть
            String className = eventType;
            if (className.endsWith("Avro")) {
                className = "ru.yandex.practicum.kafka.telemetry.event." + className;
            } else {
                className = "ru.yandex.practicum.kafka.telemetry.event." + className + "Avro";
            }

            log.debug("Loading class: {}", className);
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Unknown event type: " + eventType, e);
        }
    }

    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(hubConsumer::wakeup));
        try {
            hubConsumer.subscribe(List.of(hubEventsTopic));
            log.info("Subscribed to topic: {}", hubEventsTopic);

            while (true) {
                ConsumerRecords<String, HubEventAvro> records = hubConsumer.poll(Duration.ofMillis(1000));
                if (!records.isEmpty()) {
                    log.info("Received {} hub event records", records.count());
                    for (ConsumerRecord<String, HubEventAvro> record : records) {
                        try {
                            HubEventAvro event = record.value();
                            Object payload = event.getPayload();
                            HubEventHandler eventHandler = hubEventHandlers.get(payload.getClass());

                            if (eventHandler != null) {
                                log.debug("Processing event with handler: {}", eventHandler.getClass().getSimpleName());
                                eventHandler.handle(event);
                            } else {
                                log.warn("No handler found for event type: {}", payload.getClass().getSimpleName());
                                // Просто логируем, не бросаем исключение
                            }
                        } catch (Exception e) {
                            log.error("Error processing Kafka record: {}", e.getMessage(), e);
                            // Продолжаем обработку следующих сообщений
                        }
                    }
                    hubConsumer.commitAsync();
                }
            }
        } catch (WakeupException ignored) {
            log.info("WakeupException caught, shutting down HubEventProcessor");
        } catch (Exception e) {
            log.error("Error in HubEventProcessor", e);
        } finally {
            try {
                hubConsumer.commitSync();
            } finally {
                hubConsumer.close();
            }
        }
    }
}