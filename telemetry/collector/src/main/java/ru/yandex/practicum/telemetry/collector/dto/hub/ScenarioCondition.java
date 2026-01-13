package ru.yandex.practicum.telemetry.collector.dto.hub;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class ScenarioCondition {
    @NotBlank(message = "sensor_id не может быть пустым")
    @JsonProperty("sensor_id")
    private String sensorId;

    @NotNull(message = "type не может быть null")
    private ConditionType type;

    @NotNull(message = "operation не может быть null")

    private int value;
}