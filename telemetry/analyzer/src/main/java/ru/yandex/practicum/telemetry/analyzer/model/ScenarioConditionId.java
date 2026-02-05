package ru.yandex.practicum.telemetry.analyzer.model;

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
public class ScenarioConditionId implements Serializable {

    @Column(name = "scenario_id")
    private Long scenario;

    @Column(name = "sensor_id")
    private String sensor;

    @Column(name = "condition_id")
    private Long condition;
}