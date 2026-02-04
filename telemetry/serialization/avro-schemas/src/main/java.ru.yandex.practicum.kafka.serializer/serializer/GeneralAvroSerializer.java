package ru.yandex.practicum.kafka.serializer.serializer;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class GeneralAvroSerializer<T extends SpecificRecordBase> implements Serializer<T> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Конфигурация не требуется
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            DatumWriter<T> writer = new SpecificDatumWriter<>(data.getSchema());
            writer.write(data, encoder);
            encoder.flush();
            outputStream.close();
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Error serializing Avro message for topic: " + topic, e);
        }
    }

    @Override
    public void close() {
        // Ничего не нужно закрывать
    }
}
