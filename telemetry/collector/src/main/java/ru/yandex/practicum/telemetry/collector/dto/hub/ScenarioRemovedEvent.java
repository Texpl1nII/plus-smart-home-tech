package ru.yandex.practicum.telemetry.collector.dto.hub;

import lombok.*;

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class ScenarioRemovedEvent extends HubEvent {

    private String name;

    public ScenarioRemovedEvent(String hubId, String name) {
        setHubId(hubId);
        this.name = name;
    }

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_REMOVED;
    }
}