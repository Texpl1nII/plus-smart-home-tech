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
        System.out.println("DEBUG: Handling hub event: " + event.getType() + " for hub: " + event.getHubId());

        if (!event.getType().equals(getMessageType())) {
            System.err.println("ERROR: Event type mismatch. Expected: " + getMessageType() + ", got: " + event.getType());
            throw new IllegalArgumentException("Неизвестный тип события: " + event.getType());
        }

        try {
            T payload = mapToAvro(event);
            System.out.println("DEBUG: Mapped to Avro payload: " + payload.getClass().getSimpleName());

            HubEventAvro eventAvro = HubEventAvro.newBuilder()
                    .setHubId(event.getHubId())
                    .setTimestamp(event.getTimestamp())
                    .setPayload(payload)
                    .build();

            System.out.println("DEBUG: Created HubEventAvro for topic: " + topic);

            ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                    topic,
                    null,
                    event.getTimestamp().toEpochMilli(),
                    eventAvro.getHubId(),
                    eventAvro);

            System.out.println("DEBUG: Sending to Kafka. Topic: " + topic + ", key: " + eventAvro.getHubId());

            producer.getProducer().send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("ERROR: Failed to send to Kafka: " + exception.getMessage());
                    exception.printStackTrace();
                } else {
                    System.out.println("SUCCESS: Sent to Kafka. Topic: " + metadata.topic() +
                            ", partition: " + metadata.partition() +
                            ", offset: " + metadata.offset());
                }
            });

            // Важно: сделать flush для немедленной отправки
            producer.getProducer().flush();

            log.info("Отправили в Kafka: hubId={}, type={}",
                    event.getHubId(), event.getType());

        } catch (Exception e) {
            System.err.println("ERROR: Exception in handle(): " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }
}
