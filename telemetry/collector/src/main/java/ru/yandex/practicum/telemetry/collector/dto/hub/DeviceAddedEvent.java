package ru.yandex.practicum.telemetry.collector.dto.hub;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
public class DeviceAddedEvent extends HubEvent {

    private String id;

    @JsonProperty("device_type")
    private DeviceType deviceType;

    public DeviceAddedEvent(String hubId, String id, DeviceType deviceType) {
        setHubId(hubId);
        this.id = id;
        this.deviceType = deviceType;
        setType(HubEventType.DEVICE_ADDED);
    }

    public DeviceAddedEvent(String hubId, String id, DeviceType deviceType, java.time.Instant timestamp) {
        setHubId(hubId);
        this.id = id;
        this.deviceType = deviceType;
        setType(HubEventType.DEVICE_ADDED);
        setTimestamp(timestamp != null ? timestamp : java.time.Instant.now());
    }
}