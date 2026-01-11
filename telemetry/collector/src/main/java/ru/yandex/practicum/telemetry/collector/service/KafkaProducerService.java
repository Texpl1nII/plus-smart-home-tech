package ru.yandex.practicum.telemetry.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.config.KafkaTopics;
import ru.yandex.practicum.telemetry.collector.dto.hub.HubEvent;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.collector.serializer.AvroSerializer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final AvroSerializer avroSerializer;

    public void sendHubEvent(HubEvent event) {
        try {
            HubEventAvro avroEvent = avroSerializer.convertToAvro(event);

            log.debug("Sending hub event to Kafka: topic={}, hubId={}, type={}",
                    KafkaTopics.HUBS_TOPIC, event.getHubId(), event.getType());

            // Синхронная отправка с таймаутом для тестов
            var future = kafkaTemplate.send(KafkaTopics.HUBS_TOPIC, event.getHubId(), avroEvent);

            // Ждем отправки (таймаут 5 секунд)
            var result = future.get(5, TimeUnit.SECONDS);

            log.debug("Hub event sent successfully: hubId={}, partition={}, offset={}",
                    event.getHubId(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while sending hub event: hubId={}", event.getHubId(), e);
            throw new RuntimeException("Interrupted while sending hub event", e);

        } catch (ExecutionException e) {
            log.error("Failed to send hub event: hubId={}", event.getHubId(), e.getCause());
            throw new RuntimeException("Failed to send hub event: " + e.getCause().getMessage(), e.getCause());

        } catch (TimeoutException e) {
            log.error("Timeout while sending hub event: hubId={}", event.getHubId(), e);
            throw new RuntimeException("Timeout while sending hub event", e);

        } catch (Exception e) {
            log.error("Error sending hub event to Kafka: hubId={}", event.getHubId(), e);
            throw new RuntimeException("Failed to send hub event", e);
        }
    }

    public void sendSensorEvent(SensorEvent event) {
        try {
            SensorEventAvro avroEvent = avroSerializer.convertToAvro(event);

            log.debug("Sending sensor event to Kafka: topic={}, sensorId={}, type={}",
                    KafkaTopics.SENSORS_TOPIC, event.getId(), event.getType());

            // Синхронная отправка с таймаутом для тестов
            var future = kafkaTemplate.send(KafkaTopics.SENSORS_TOPIC, event.getId(), avroEvent);

            // Ждем отправки (таймаут 5 секунд)
            var result = future.get(5, TimeUnit.SECONDS);

            log.debug("Sensor event sent successfully: sensorId={}, partition={}, offset={}",
                    event.getId(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while sending sensor event: sensorId={}", event.getId(), e);
            throw new RuntimeException("Interrupted while sending sensor event", e);

        } catch (ExecutionException e) {
            log.error("Failed to send sensor event: sensorId={}", event.getId(), e.getCause());
            throw new RuntimeException("Failed to send sensor event: " + e.getCause().getMessage(), e.getCause());

        } catch (TimeoutException e) {
            log.error("Timeout while sending sensor event: sensorId={}", event.getId(), e);
            throw new RuntimeException("Timeout while sending sensor event", e);

        } catch (Exception e) {
            log.error("Error sending sensor event to Kafka: sensorId={}", event.getId(), e);
            throw new RuntimeException("Failed to send sensor event", e);
        }
    }
}
