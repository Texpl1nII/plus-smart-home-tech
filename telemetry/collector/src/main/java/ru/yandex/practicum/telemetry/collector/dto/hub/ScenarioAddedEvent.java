package ru.yandex.practicum.telemetry.collector.dto.hub;

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

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_ADDED_EVENT;
    }
}
