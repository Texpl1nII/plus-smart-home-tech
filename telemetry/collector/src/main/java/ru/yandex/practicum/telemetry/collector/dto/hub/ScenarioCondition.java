package ru.yandex.practicum.telemetry.collector.dto.hub;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class ScenarioCondition {
    @JsonProperty("sensor_id")
    private String sensorId;

    private ConditionType type;

    private ConditionOperation operation;

    private Object value;
}