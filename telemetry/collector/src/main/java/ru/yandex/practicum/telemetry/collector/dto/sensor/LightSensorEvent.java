package ru.yandex.practicum.telemetry.collector.dto.sensor;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.*;

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
public class LightSensorEvent extends SensorEvent {

    @Min(value = 0, message = "linkQuality must be at least 0")
    @Max(value = 100, message = "linkQuality must be at most 100")
    private int linkQuality;

    @Min(value = 0, message = "luminosity must be at least 0")
    @Max(value = 100, message = "luminosity must be at most 100")
    private int luminosity;

    @Override
    public SensorEventType getType() {
        return SensorEventType.LIGHT_SENSOR_EVENT;
    }
}