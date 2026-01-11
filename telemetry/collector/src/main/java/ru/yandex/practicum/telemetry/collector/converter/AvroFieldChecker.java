package ru.yandex.practicum.telemetry.collector.converter;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.lang.reflect.Method;
import java.util.Arrays;

@Slf4j
@Component
public class AvroFieldChecker {

    @PostConstruct
    public void checkFields() {
        log.info("=== CHECKING AVRO CLASSES ===");

        log.info("HubEventAvro Builder methods:");
        Method[] hubMethods = HubEventAvro.Builder.class.getMethods();
        Arrays.stream(hubMethods)
                .filter(m -> m.getName().startsWith("set"))
                .sorted((m1, m2) -> m1.getName().compareTo(m2.getName()))
                .forEach(m -> log.info("  - {}", m.getName()));

        log.info("\nSensorEventAvro Builder methods:");
        Method[] sensorMethods = SensorEventAvro.Builder.class.getMethods();
        Arrays.stream(sensorMethods)
                .filter(m -> m.getName().startsWith("set"))
                .sorted((m1, m2) -> m1.getName().compareTo(m2.getName()))
                .forEach(m -> log.info("  - {}", m.getName()));

        log.info("\nHubEventAvro schema fields:");
        HubEventAvro.getClassSchema().getFields().forEach(field -> {
            log.info("  - {}: {}", field.name(), field.schema().getType());
        });

        log.info("\nSensorEventAvro schema fields:");
        SensorEventAvro.getClassSchema().getFields().forEach(field -> {
            log.info("  - {}: {}", field.name(), field.schema().getType());
        });
    }
}
