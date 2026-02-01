package ru.yandex.practicum.telemetry.analyzer.deserializer;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import ru.yandex.practicum.kafka.telemetry.event.HubEvent;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class HubEventDeserializer implements Deserializer<HubEvent> {

    @Override
    public HubEvent deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data)) {
            DatumReader<HubEvent> reader = new SpecificDatumReader<>(HubEvent.class);
            Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new SerializationException("Error deserializing HubEvent", e);
        }
    }
}
