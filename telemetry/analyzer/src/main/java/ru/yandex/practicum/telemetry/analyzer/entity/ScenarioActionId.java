package ru.yandex.practicum.telemetry.analyzer.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@Embeddable
public class ScenarioActionId implements Serializable {

    @Column(name = "scenario_id")
    private Long scenarioId;

    @Column(name = "sensor_id")
    private String sensorId;

    @Column(name = "action_id")
    private Long actionId;
}
