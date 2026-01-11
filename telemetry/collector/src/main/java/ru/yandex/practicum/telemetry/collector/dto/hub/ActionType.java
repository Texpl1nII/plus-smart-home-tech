package ru.yandex.practicum.telemetry.collector.dto.hub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum ActionType {
    ACTIVATE("ACTIVATE"),
    DEACTIVATE("DEACTIVATE"),
    INVERSE("INVERSE"),
    SET_VALUE("SET_VALUE");

    private final String jsonValue;

    ActionType(String jsonValue) {
        this.jsonValue = jsonValue;
    }

    @JsonValue
    public String getJsonValue() {
        return jsonValue;
    }

    @JsonCreator
    public static ActionType fromJson(String value) {
        for (ActionType type : values()) {
            if (type.jsonValue.equals(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown ActionType: " + value);
    }
}
