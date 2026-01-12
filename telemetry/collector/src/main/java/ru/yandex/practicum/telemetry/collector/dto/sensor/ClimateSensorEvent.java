package ru.yandex.practicum.telemetry.collector.dto.sensor;

import lombok.*;

import jakarta.validation.constraints.NotNull;

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
public class ClimateSensorEvent extends SensorEvent {

    @NotNull
    private int temperatureC;

    @NotNull
    private int humidity;

    @NotNull
    private int co2Level;

    @Override
    public SensorEventType getType() {
        return SensorEventType.CLIMATE_SENSOR_EVENT;
    }
}