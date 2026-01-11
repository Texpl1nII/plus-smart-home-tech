package ru.yandex.practicum.telemetry.collector.dto.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class SensorEventRequest {

    @NotBlank(message = "id must not be blank")
    private String id;

    @NotBlank(message = "hub_id must not be blank")
    @JsonProperty("hub_id")
    private String hubId;

    @NotBlank(message = "type must not be blank")
    private String type; // "LIGHT_SENSOR_EVENT", "CLIMATE_SENSOR_EVENT", etc.

    // Общие поля (могут быть null)
    @Min(value = 0, message = "linkQuality must be at least 0")
    @Max(value = 100, message = "linkQuality must be at most 100")
    @JsonProperty("link_quality")
    private Integer linkQuality;

    // Для LIGHT_SENSOR_EVENT
    @Min(value = 0, message = "luminosity must be at least 0")
    @Max(value = 100, message = "luminosity must be at most 100")
    private Integer luminosity;

    // Для CLIMATE_SENSOR_EVENT
    @JsonProperty("temperature_c")
    private Integer temperatureC;

    private Integer humidity;

    @JsonProperty("co2_level")
    private Integer co2Level;

    // Для MOTION_SENSOR_EVENT
    private Boolean motion;
    private Integer voltage;

    // Для SWITCH_SENSOR_EVENT
    private Boolean state;

    // Для TEMPERATURE_SENSOR_EVENT
    @JsonProperty("temperature_f")
    private Integer temperatureF;
}