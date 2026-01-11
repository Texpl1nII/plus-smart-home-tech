package ru.yandex.practicum.telemetry.collector.dto.sensor;

import lombok.*;

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class SwitchSensorEvent extends SensorEvent {

    private boolean state;  // примитив boolean

    @Override
    public SensorEventType getType() {
        return SensorEventType.SWITCH_SENSOR_EVENT;
    }
}