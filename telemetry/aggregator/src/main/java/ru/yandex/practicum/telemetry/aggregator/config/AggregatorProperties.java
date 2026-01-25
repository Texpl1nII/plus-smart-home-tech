package ru.yandex.practicum.telemetry.aggregator.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "aggregator")
public class AggregatorProperties {
    private String inputTopic = "telemetry.sensors.v1";
    private String outputTopic = "telemetry.snapshots.v1";
    private String groupId = "aggregator-group";
}
