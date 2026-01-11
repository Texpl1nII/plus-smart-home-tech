package ru.yandex.practicum.telemetry.collector.dto.sensor;

import lombok.*;

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
public class MotionSensorEvent extends SensorEvent {

    private int linkQuality;  // примитив int
    private boolean motion;   // примитив boolean
    private int voltage;      // примитив int

    @Override
    public SensorEventType getType() {
        return SensorEventType.MOTION_SENSOR_EVENT;
    }
}
