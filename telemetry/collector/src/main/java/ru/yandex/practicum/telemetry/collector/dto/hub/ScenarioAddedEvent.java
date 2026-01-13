package ru.yandex.practicum.telemetry.collector.dto.hub;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.util.List;

@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
public class ScenarioAddedEvent extends HubEvent {
    @NotBlank(message = "name не может быть пустым")
    private String name;

    @NotNull(message = "conditions не может быть null")
    @Valid  // ВАЖНО: для валидации вложенных объектов!
    private List<ScenarioCondition> conditions;

    @NotNull(message = "actions не может быть null")
    @Valid  // ВАЖНО: для валидации вложенных объектов!
    private List<DeviceAction> actions;

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_ADDED;
    }
}