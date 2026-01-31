package ru.yandex.practicum.telemetry.analyzer.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.telemetry.analyzer.deserializer.HubEventDeserializer;
import ru.yandex.practicum.telemetry.analyzer.handler.HubEventHandler;
import ru.yandex.practicum.telemetry.analyzer.kafka.KafkaClient;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Component
public class HubEventProcessor implements Runnable {

    private final KafkaClient kafkaClient;
    private final HubEventDeserializer hubEventDeserializer;
    private final Map<String, HubEventHandler> hubEventHandlers;

    @Value("${analyzer.kafka.topics.hub-events}")
    private String hubEventsTopic;

    public HubEventProcessor(KafkaClient kafkaClient,
                             List<HubEventHandler> hubEventHandlers,
                             HubEventDeserializer hubEventDeserializer) {
        this.kafkaClient = kafkaClient;
        this.hubEventHandlers = hubEventHandlers.stream()
                .collect(Collectors.toMap(HubEventHandler::getEventType, Function.identity()));
        this.hubEventDeserializer = hubEventDeserializer;
    }

    @Override
    public void run() {
        var consumer = kafkaClient.getHubConsumer();

        // Добавляем shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        try {
            consumer.subscribe(List.of(hubEventsTopic));
            log.info("HubEventProcessor started, subscribed to topic: {}", hubEventsTopic);

            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));

                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, byte[]> record : records) {
                        try {
                            // Ручная десериализация
                            HubEventAvro event = hubEventDeserializer.deserialize(
                                    record.topic(), record.value());

                            String eventPayloadName = event.getPayload().getClass().getSimpleName();
                            HubEventHandler eventHandler = hubEventHandlers.get(eventPayloadName);

                            if (eventHandler != null) {
                                eventHandler.handle(event);
                                log.debug("Processed hub event: {} for hub: {}",
                                        eventPayloadName, event.getHubId());
                            } else {
                                log.warn("No handler found for event type: {}", eventPayloadName);
                            }
                        } catch (Exception e) {
                            log.error("Error processing hub event: {}", e.getMessage(), e);
                        }
                    }
                    consumer.commitAsync();
                }
            }
        } catch (WakeupException e) {
            log.info("HubEventProcessor received wakeup signal");
        } catch (Exception e) {
            log.error("Error in HubEventProcessor", e);
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
                log.info("HubEventProcessor stopped");
            }
        }
    }
}