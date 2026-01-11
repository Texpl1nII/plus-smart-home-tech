package ru.yandex.practicum.telemetry.collector.dto.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class HubEventRequest {

    @NotBlank(message = "hub_id must not be blank")
    @JsonProperty("hub_id")
    private String hubId;

    @NotBlank(message = "type must not be blank")
    private String type; // "DEVICE_ADDED", "DEVICE_REMOVED", "SCENARIO_ADDED", "SCENARIO_REMOVED"

    // Для DEVICE_ADDED и DEVICE_REMOVED
    private String id;

    // Только для DEVICE_ADDED
    @JsonProperty("device_type")
    private String deviceType; // "LIGHT_SENSOR", "MOTION_SENSOR", etc.

    // Только для SCENARIO_ADDED и SCENARIO_REMOVED
    private String name;

    // Для SCENARIO_ADDED (упрощенно)
    private String conditions;
    private String actions;
}