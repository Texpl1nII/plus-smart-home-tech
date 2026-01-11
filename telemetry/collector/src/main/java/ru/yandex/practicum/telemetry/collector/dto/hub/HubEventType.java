package ru.yandex.practicum.telemetry.collector.dto.hub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum HubEventType {
    DEVICE_ADDED_EVENT("DEVICE_ADDED"),
    DEVICE_REMOVED_EVENT("DEVICE_REMOVED"),
    SCENARIO_ADDED_EVENT("SCENARIO_ADDED"),
    SCENARIO_REMOVED_EVENT("SCENARIO_REMOVED");

    private final String jsonValue;

    HubEventType(String jsonValue) {
        this.jsonValue = jsonValue;
    }

    @JsonValue  // При сериализации в JSON → "DEVICE_ADDED"
    public String getJsonValue() {
        return jsonValue;
    }

    @JsonCreator  // При десериализации из JSON "DEVICE_ADDED" → DEVICE_ADDED_EVENT
    public static HubEventType fromJson(String value) {
        for (HubEventType type : values()) {
            if (type.jsonValue.equals(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown HubEventType: " + value);
    }
}