package ru.yandex.practicum.telemetry.collector.dto.sensor;

import lombok.*;

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class ClimateSensorEvent extends SensorEvent {

    private int temperatureC;
    private int humidity;
    private int co2Level;

    @Override
    public SensorEventType getType() {
        return SensorEventType.CLIMATE_SENSOR_EVENT;
    }
}
