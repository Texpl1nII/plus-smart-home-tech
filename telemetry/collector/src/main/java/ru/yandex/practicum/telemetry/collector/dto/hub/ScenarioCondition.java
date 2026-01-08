package ru.yandex.practicum.telemetry.collector.dto.hub;

import lombok.*;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class ScenarioCondition {
    private String sensorId;
    private ConditionType type;
    private ConditionOperation operation;
    private Object value; // Может изменить на Integer, Boolean или null*
}