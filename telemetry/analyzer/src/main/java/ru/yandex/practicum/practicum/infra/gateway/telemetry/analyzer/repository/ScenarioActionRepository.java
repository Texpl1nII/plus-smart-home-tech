package ru.yandex.practicum.practicum.infra.gateway.telemetry.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.practicum.infra.gateway.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.practicum.infra.gateway.telemetry.analyzer.model.ScenarioAction;
import ru.yandex.practicum.practicum.infra.gateway.telemetry.analyzer.model.ScenarioActionId;

import java.util.List;

@Repository
public interface ScenarioActionRepository extends JpaRepository<ScenarioAction, ScenarioActionId> {
    List<ScenarioAction> findByScenario(Scenario scenario);
    void deleteByScenario(Scenario scenario);
}