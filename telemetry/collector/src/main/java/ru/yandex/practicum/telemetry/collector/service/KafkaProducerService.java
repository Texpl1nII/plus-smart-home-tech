package ru.yandex.practicum.telemetry.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.config.KafkaTopics;
import ru.yandex.practicum.telemetry.collector.dto.hub.HubEvent;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.collector.serializer.AvroSerializer;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final AvroSerializer avroSerializer;

    public void sendSensorEvent(SensorEvent event) {
        try {
            // Конвертируем DTO в Avro
            SensorEventAvro avroEvent = avroSerializer.convertToAvro(event);

            // Отправляем в Kafka
            CompletableFuture<SendResult<String, Object>> future =
                    kafkaTemplate.send(KafkaTopics.SENSORS_TOPIC, event.getId(), avroEvent);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.debug("Sensor event sent successfully: {}", event.getId());
                } else {
                    log.error("Failed to send sensor event: {}", event.getId(), ex);
                }
            });

        } catch (Exception e) {
            log.error("Error processing sensor event: {}", event.getId(), e);
            throw new RuntimeException("Failed to process sensor event", e);
        }
    }

    public void sendHubEvent(HubEvent event) {
        try {
            // Конвертируем DTO в Avro
            HubEventAvro avroEvent = avroSerializer.convertToAvro(event);

            // Отправляем в Kafka
            CompletableFuture<SendResult<String, Object>> future =
                    kafkaTemplate.send(KafkaTopics.HUBS_TOPIC, event.getHubId(), avroEvent);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.debug("Hub event sent successfully: {}", event.getHubId());
                } else {
                    log.error("Failed to send hub event: {}", event.getHubId(), ex);
                }
            });

        } catch (Exception e) {
            log.error("Error processing hub event: {}", event.getHubId(), e);
            throw new RuntimeException("Failed to process hub event", e);
        }
    }
}
