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

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final AvroSerializer avroSerializer;

    public void sendHubEvent(HubEvent event) {
        try {
            // Конвертируем в Avro
            HubEventAvro avroEvent = avroSerializer.convertToAvro(event);

            log.info("Sending hub event to Kafka: topic={}, hubId={}",
                    KafkaTopics.HUBS_TOPIC, event.getHubId());

            kafkaTemplate.send(KafkaTopics.HUBS_TOPIC, event.getHubId(), avroEvent)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("Hub event sent successfully: hubId={}, partition={}, offset={}",
                                    event.getHubId(),
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        } else {
                            log.error("Failed to send hub event: hubId={}", event.getHubId(), ex);
                        }
                    });

        } catch (Exception e) {
            log.error("Error sending hub event to Kafka: {}", event.getHubId(), e);
            throw new RuntimeException("Failed to send hub event", e);
        }
    }

    public void sendSensorEvent(SensorEvent event) {
        try {
            // Конвертируем в Avro
            SensorEventAvro avroEvent = avroSerializer.convertToAvro(event);

            log.info("Sending sensor event to Kafka: topic={}, sensorId={}",
                    KafkaTopics.SENSORS_TOPIC, event.getId());

            kafkaTemplate.send(KafkaTopics.SENSORS_TOPIC, event.getId(), avroEvent)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("Sensor event sent successfully: sensorId={}, partition={}, offset={}",
                                    event.getId(),
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        } else {
                            log.error("Failed to send sensor event: sensorId={}", event.getId(), ex);
                        }
                    });

        } catch (Exception e) {
            log.error("Error sending sensor event to Kafka: {}", event.getId(), e);
            throw new RuntimeException("Failed to send sensor event", e);
        }
    }
}
