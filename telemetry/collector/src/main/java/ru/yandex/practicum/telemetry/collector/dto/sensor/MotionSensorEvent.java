package ru.yandex.practicum.telemetry.collector.dto.sensor;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
public class MotionSensorEvent extends SensorEvent {

    @JsonProperty("link_quality")
    private Integer linkQuality;

    private Boolean motion;
    private Integer voltage;

    @Override
    public SensorEventType getType() {
        return SensorEventType.MOTION_SENSOR_EVENT;
    }
}
