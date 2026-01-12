package ru.yandex.practicum.telemetry.collector.service.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaClientProducer;
import ru.yandex.practicum.telemetry.collector.dto.hub.HubEvent;

@Slf4j
@RequiredArgsConstructor
public abstract class BaseHubEventHandler<T extends SpecificRecordBase> implements HubEventHandler {
    protected final KafkaClientProducer producer;

    @Value("${kafka.topic.hub}")
    protected String topic;

    protected abstract T mapToAvro(HubEvent event);

    @Override
    public void handle(HubEvent event) {
        if (!event.getType().equals(getMessageType())) {
            throw new IllegalArgumentException("Неизвестный тип события: " + event.getType());
        }

        try {
            T payload = mapToAvro(event);

            HubEventAvro eventAvro = HubEventAvro.newBuilder()
                    .setHubId(event.getHubId())
                    .setTimestamp(event.getTimestamp())  // Instant!
                    .setPayload(payload)
                    .build();

            ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                    topic,
                    null,
                    event.getTimestamp().toEpochMilli(),  // для Kafka timestamp используем миллисекунды
                    eventAvro.getHubId(),
                    eventAvro);

            producer.getProducer().send(record);

            log.info("Отправили в Kafka: hubId={}, timestamp={}, type={}",
                    event.getHubId(), event.getTimestamp(), event.getType());
        } catch (Exception e) {
            log.error("Error handling hub event: hubId={}, type={}",
                    event.getHubId(), event.getType(), e);
            throw e;
        }
    }
}
