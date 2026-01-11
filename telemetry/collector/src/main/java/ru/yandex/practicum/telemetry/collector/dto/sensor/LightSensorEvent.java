package ru.yandex.practicum.telemetry.collector.dto.sensor;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.*;

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
public class LightSensorEvent extends SensorEvent {

    @Min(0)
    @Max(100)
    private int linkQuality;

    @Min(0)
    @Max(100)
    private int luminosity;

    @Override
    public SensorEventType getType() {
        return SensorEventType.LIGHT_SENSOR_EVENT;
    }
}