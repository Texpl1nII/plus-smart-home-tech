package ru.yandex.practicum.telemetry.analyzer.entity;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "scenario_actions")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ScenarioAction {

    @EmbeddedId
    private ScenarioActionId id;

    @MapsId("scenarioId")
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "scenario_id", nullable = false)
    private Scenario scenario;

    @MapsId("sensorId")
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "sensor_id", nullable = false)
    private Sensor sensor;

    @MapsId("actionId")
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "action_id", nullable = false)
    private Action action;
}