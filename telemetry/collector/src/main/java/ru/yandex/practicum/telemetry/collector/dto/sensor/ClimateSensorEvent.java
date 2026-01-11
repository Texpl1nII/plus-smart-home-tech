package ru.yandex.practicum.telemetry.collector.dto.sensor;

import lombok.*;

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
public class ClimateSensorEvent extends SensorEvent {

    private int temperatureC;  // примитив int
    private int humidity;      // примитив int
    private int co2Level;      // примитив int

    @Override
    public SensorEventType getType() {
        return SensorEventType.CLIMATE_SENSOR_EVENT;
    }
}