package ru.yandex.practicum.telemetry.collector.dto.hub;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import jakarta.validation.constraints.NotBlank;

@Getter
@Setter
@ToString(callSuper = true)
public class DeviceAction {
    @NotBlank
    private String sensorId;
    private ActionType type;
    private Integer value;
}