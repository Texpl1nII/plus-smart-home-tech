package ru.yandex.practicum.telemetry.collector.dto.hub;

import lombok.*;

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
public class DeviceAddedEvent extends HubEvent {

    private String id;
    private DeviceType type;

    @Override
    public HubEventType getType() {
        return HubEventType.DEVICE_ADDED_EVENT;
    }
}