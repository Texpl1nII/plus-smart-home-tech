package ru.yandex.practicum.practicum.infra.gateway.telemetry.collector.dto.hub;

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