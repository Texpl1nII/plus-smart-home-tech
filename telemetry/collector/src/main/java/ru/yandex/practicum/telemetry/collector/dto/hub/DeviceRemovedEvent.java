package ru.yandex.practicum.telemetry.collector.dto.hub;

import lombok.*;

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class DeviceRemovedEvent extends HubEvent {

    private String id;

    @Override
    public HubEventType getType() {
        return HubEventType.DEVICE_REMOVED_EVENT;
    }
}
