package ru.yandex.practicum.telemetry.collector.dto.sensor;

import lombok.*;

import jakarta.validation.constraints.NotNull;

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
public class MotionSensorEvent extends SensorEvent {

    @NotNull
    private int linkQuality;

    @NotNull
    private boolean motion;

    @NotNull
    private int voltage;

    @Override
    public SensorEventType getType() {
        return SensorEventType.MOTION_SENSOR_EVENT;
    }
}