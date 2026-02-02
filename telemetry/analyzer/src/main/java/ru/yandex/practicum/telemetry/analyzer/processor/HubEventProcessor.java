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
    private final Map<String, HubEventHandler> hubEventHandlers;

    @Value("${analyzer.kafka.topics.hub-events}")
    private String hubEventsTopic;

    public HubEventProcessor(KafkaClient kafkaClient, List<HubEventHandler> hubEventHandlers) {
        this.hubConsumer = kafkaClient.getHubConsumer();
        this.hubEventHandlers = hubEventHandlers.stream()
                .collect(Collectors.toMap(HubEventHandler::getEventType, Function.identity()));
    }

    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(hubConsumer::wakeup));
        try {
            hubConsumer.subscribe(List.of(hubEventsTopic));
            while (true) {
                ConsumerRecords<String, HubEventAvro> records = hubConsumer.poll(Duration.ofMillis(1000));
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, HubEventAvro> record : records) {
                        HubEventAvro event = record.value();
                        String eventPayloadName = event.getPayload().getClass().getSimpleName();
                        HubEventHandler eventHandler;

                        if (hubEventHandlers.containsKey(eventPayloadName)) {
                            eventHandler = hubEventHandlers.get(eventPayloadName);
                        } else {
                            throw new IllegalArgumentException("Подходящий handler не найден");
                        }
                        eventHandler.handle(event);

                    }
                    hubConsumer.commitAsync();
                }
            }
        } catch (WakeupException ignored) {

        } catch (Exception e) {
            log.error("Ошибка при обработке событий", e);
        } finally {
            try {
                hubConsumer.commitSync();
            } finally {
                hubConsumer.close();
            }
        }
    }
}