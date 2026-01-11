package ru.yandex.practicum.telemetry.collector.dto.sensor;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
public class ClimateSensorEvent extends SensorEvent {

    @JsonProperty("temperature_c")
    private Integer temperatureC;

    private Integer humidity;

    @JsonProperty("co2_level")
    private Integer co2Level;

    @Override
    public SensorEventType getType() {
        return SensorEventType.CLIMATE_SENSOR_EVENT;
    }
}
