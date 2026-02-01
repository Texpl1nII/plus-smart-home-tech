package ru.yandex.practicum.telemetry.analyzer.entity;

import jakarta.persistence.*;
import lombok.*;

import java.util.HashSet;
import java.util.Set;

@Entity
@Table(name = "sensors")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Sensor {

    @Id
    @Column(name = "id")
    private String id;

    @Column(name = "hub_id")
    private String hubId;

    @OneToMany(mappedBy = "sensor", cascade = CascadeType.ALL, orphanRemoval = true)
    @Builder.Default
    private Set<ScenarioCondition> scenarioConditions = new HashSet<>();

    @OneToMany(mappedBy = "sensor", cascade = CascadeType.ALL, orphanRemoval = true)
    @Builder.Default
    private Set<ScenarioAction> scenarioActions = new HashSet<>();
}