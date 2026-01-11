package ru.yandex.practicum.telemetry.collector.dto.hub;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.time.Instant;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type",
        defaultImpl = HubEventType.class,
        visible = true
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = DeviceAddedEvent.class, name = "DEVICE_ADDED"),
        @JsonSubTypes.Type(value = DeviceRemovedEvent.class, name = "DEVICE_REMOVED"),
        @JsonSubTypes.Type(value = ScenarioAddedEvent.class, name = "SCENARIO_ADDED"),
        @JsonSubTypes.Type(value = ScenarioRemovedEvent.class, name = "SCENARIO_REMOVED")
})
@Getter
@Setter
@ToString
@NoArgsConstructor
public abstract class HubEvent {

    @NotBlank(message = "hub_id must not be blank")
    @JsonAlias({"hub_id", "hubId"})  // Принимает оба варианта
    @JsonProperty("hub_id")          // В Kafka отправляем как hub_id
    private String hubId;

    private Instant timestamp = Instant.now();

    @JsonProperty("type")
    @NotNull
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private HubEventType type;

    public HubEventType getType() {
        return type;
    }

    protected void setType(HubEventType type) {
        this.type = type;
    }
}
