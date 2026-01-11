package ru.yandex.practicum.telemetry.collector.dto.hub;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.util.List;

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class ScenarioAddedEvent extends HubEvent {

    private String name;

    private List<ScenarioCondition> conditions;

    private List<DeviceAction> actions;

    public ScenarioAddedEvent(String hubId, String name,
                              List<ScenarioCondition> conditions,
                              List<DeviceAction> actions) {
        setHubId(hubId);
        this.name = name;
        this.conditions = conditions;
        this.actions = actions;
        setType(HubEventType.SCENARIO_ADDED);
    }
}