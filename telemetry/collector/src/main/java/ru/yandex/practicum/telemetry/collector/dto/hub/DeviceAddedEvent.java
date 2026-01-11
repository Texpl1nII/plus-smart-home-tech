package ru.yandex.practicum.telemetry.collector.dto.hub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.time.Instant;

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
public class DeviceAddedEvent extends HubEvent {

    private String id;
    private DeviceType deviceType;  // camelCase!

    @JsonCreator
    public static DeviceAddedEvent create(
            @JsonProperty("hubId") String hubId,
            @JsonProperty("id") String id,
            @JsonProperty("deviceType") String deviceTypeStr,
            @JsonProperty("timestamp") Instant timestamp) {

        DeviceAddedEvent event = new DeviceAddedEvent();
        event.setHubId(hubId);
        event.setId(id);
        event.setDeviceType(deviceTypeStr != null ? DeviceType.fromJson(deviceTypeStr) : null);
        event.setType(HubEventType.DEVICE_ADDED);
        event.setTimestamp(timestamp != null ? timestamp : Instant.now());
        return event;
    }

    @Override
    public HubEventType getType() {
        return HubEventType.DEVICE_ADDED;
    }
}