package ru.yandex.practicum.telemetry.collector.dto.hub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum DeviceType {
    MOTION_SENSOR("MOTION_SENSOR"),
    TEMPERATURE_SENSOR("TEMPERATURE_SENSOR"),
    LIGHT_SENSOR("LIGHT_SENSOR"),
    CLIMATE_SENSOR("CLIMATE_SENSOR"),
    SWITCH_SENSOR("SWITCH_SENSOR");

    private final String jsonValue;

    DeviceType(String jsonValue) {
        this.jsonValue = jsonValue;
    }

    @JsonValue
    public String getJsonValue() {
        return jsonValue;
    }

    @JsonCreator
    public static DeviceType fromJson(String value) {
        if (value == null) return null;

        for (DeviceType type : values()) {
            if (type.jsonValue.equals(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown DeviceType: " + value);
    }
}
