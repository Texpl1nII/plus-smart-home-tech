package ru.yandex.practicum.telemetry.collector.dto.sensor;

import lombok.*;

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
public class ClimateSensorEvent extends SensorEvent {

    private Integer temperatureC;  // camelCase - как temperatureC в спецификации!
    private Integer humidity;
    private Integer co2Level;

    @Override
    public SensorEventType getType() {
        return SensorEventType.CLIMATE_SENSOR_EVENT;
    }
}