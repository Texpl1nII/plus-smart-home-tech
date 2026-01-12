package ru.yandex.practicum.telemetry.collector.dto.sensor;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import jakarta.validation.constraints.NotNull;

@Getter
@Setter
@ToString(callSuper = true)
public class LightSensorEvent extends SensorEvent {
    @NotNull
    private int linkQuality;
    @NotNull
    private int luminosity;

    @Override
    public SensorEventType getType() {
        return SensorEventType.LIGHT_SENSOR_EVENT;
    }
}