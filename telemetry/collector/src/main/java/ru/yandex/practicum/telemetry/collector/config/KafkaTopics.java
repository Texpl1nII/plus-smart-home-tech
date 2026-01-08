package ru.yandex.practicum.telemetry.collector.config;

import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopics {

    public static final String SENSORS_TOPIC = "telemetry.sensors.v1";
    public static final String HUBS_TOPIC = "telemetry.hubs.v1";

    // Топик из compose.yaml (пока не используется)
    public static final String SNAPSHOTS_TOPIC = "telemetry.snapshots.v1";
}
