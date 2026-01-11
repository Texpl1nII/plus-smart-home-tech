package ru.yandex.practicum.telemetry.collector.dto.sensor;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
public class TemperatureSensorEvent extends SensorEvent {

    @JsonProperty("temperature_c")
    private Integer temperatureC;

    @JsonProperty("temperature_f")
    private Integer temperatureF;

    @Override
    public SensorEventType getType() {
        return SensorEventType.TEMPERATURE_SENSOR_EVENT;
    }
}
