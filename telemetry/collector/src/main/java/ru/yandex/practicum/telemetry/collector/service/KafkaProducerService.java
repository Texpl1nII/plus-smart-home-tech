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
            HubEventAvro avroEvent = avroSerializer.convertToAvro(event);

            log.info("Sending hub event to Kafka: topic={}, hubId={}, type={}",
                    KafkaTopics.HUBS_TOPIC, event.getHubId(), event.getType());

            // Асинхронная отправка (не блокируем поток)
            kafkaTemplate.send(KafkaTopics.HUBS_TOPIC, event.getHubId(), avroEvent)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.debug("Hub event sent successfully: hubId={}, partition={}, offset={}",
                                    event.getHubId(),
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        } else {
                            log.error("Failed to send hub event: hubId={}", event.getHubId(), ex);
                            // Не бросаем исключение здесь, чтобы не прерывать основной поток
                        }
                    });

        } catch (Exception e) {
            log.error("Error converting or sending hub event: hubId={}", event.getHubId(), e);
            throw new RuntimeException("Failed to process hub event: " + e.getMessage(), e);
        }
    }

    public void sendSensorEvent(SensorEvent event) {
        try {
            SensorEventAvro avroEvent = avroSerializer.convertToAvro(event);

            log.info("Sending sensor event to Kafka: topic={}, sensorId={}, type={}",
                    KafkaTopics.SENSORS_TOPIC, event.getId(), event.getType());

            // Асинхронная отправка (не блокируем поток)
            kafkaTemplate.send(KafkaTopics.SENSORS_TOPIC, event.getId(), avroEvent)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.debug("Sensor event sent successfully: sensorId={}, partition={}, offset={}",
                                    event.getId(),
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        } else {
                            log.error("Failed to send sensor event: sensorId={}", event.getId(), ex);
                            // Не бросаем исключение здесь, чтобы не прерывать основной поток
                        }
                    });

        } catch (Exception e) {
            log.error("Error converting or sending sensor event: sensorId={}", event.getId(), e);
            throw new RuntimeException("Failed to process sensor event: " + e.getMessage(), e);
        }
    }
}
