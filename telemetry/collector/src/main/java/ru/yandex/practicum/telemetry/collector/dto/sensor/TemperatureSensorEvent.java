package ru.yandex.practicum.telemetry.collector.dto.sensor;

import lombok.*;

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
public class TemperatureSensorEvent extends SensorEvent {

    private Integer temperatureC;  // camelCase!
    private Integer temperatureF;  // camelCase!

    @Override
    public SensorEventType getType() {
        return SensorEventType.TEMPERATURE_SENSOR_EVENT;
    }
}
