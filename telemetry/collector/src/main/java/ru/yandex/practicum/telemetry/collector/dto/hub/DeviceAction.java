package ru.yandex.practicum.telemetry.collector.dto.hub;

import lombok.*;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class DeviceAction {
    private String sensorId;
    private ActionType type;
    private Integer value; // Может быть null
}
