package ru.yandex.practicum.telemetry.collector.dto.hub;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class DeviceAction {
    @JsonProperty("sensor_id")
    private String sensorId;

    private ActionType type;

    private int value;
}