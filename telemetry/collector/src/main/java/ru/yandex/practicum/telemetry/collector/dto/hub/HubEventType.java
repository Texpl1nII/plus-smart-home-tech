package ru.yandex.practicum.telemetry.collector.dto.hub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum HubEventType {
    DEVICE_ADDED("DEVICE_ADDED"),
    DEVICE_REMOVED("DEVICE_REMOVED"),
    SCENARIO_ADDED("SCENARIO_ADDED"),
    SCENARIO_REMOVED("SCENARIO_REMOVED");

    private final String jsonValue;

    HubEventType(String jsonValue) {
        this.jsonValue = jsonValue;
    }

    @JsonValue
    public String getJsonValue() {
        return jsonValue;
    }

    @JsonCreator
    public static HubEventType fromJson(String value) {
        for (HubEventType type : values()) {
            if (type.jsonValue.equals(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown HubEventType: " + value);
    }
}