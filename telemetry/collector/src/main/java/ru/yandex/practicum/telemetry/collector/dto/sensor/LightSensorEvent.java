package ru.yandex.practicum.telemetry.collector.dto.sensor;

import com.fasterxml.jackson.annotation.JsonProperty;
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
    @JsonProperty("link_quality")
    private Integer linkQuality;  // Изменил на Integer для nullable

    @Min(0)
    @JsonProperty("luminosity")
    private Integer luminosity;  // Убрал @Max(100) и изменил на Integer

    @Override
    public SensorEventType getType() {
        return SensorEventType.LIGHT_SENSOR_EVENT;
    }
}