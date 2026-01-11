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

    @JsonProperty("device_type")
    private DeviceType deviceType;

    @JsonCreator
    public DeviceAddedEvent(@JsonProperty("hub_id") String hubId,
                            @JsonProperty("id") String id,
                            @JsonProperty("device_type") DeviceType deviceType,
                            @JsonProperty("timestamp") Instant timestamp) {
        this.setHubId(hubId);
        this.id = id;
        this.deviceType = deviceType;
        this.setType(HubEventType.DEVICE_ADDED);
        this.setTimestamp(timestamp != null ? timestamp : Instant.now());
    }
}