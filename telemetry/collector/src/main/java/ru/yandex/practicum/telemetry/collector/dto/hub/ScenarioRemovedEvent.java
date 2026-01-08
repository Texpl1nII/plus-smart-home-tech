package ru.yandex.practicum.telemetry.collector.dto.hub;

import lombok.*;

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class ScenarioRemovedEvent extends HubEvent {

    private String name;

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_REMOVED_EVENT;
    }
}