package ru.yandex.practicum.telemetry.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.telemetry.analyzer.entity.Scenario;
import ru.yandex.practicum.telemetry.analyzer.entity.ScenarioAction;
import ru.yandex.practicum.telemetry.analyzer.entity.ScenarioActionId;

import java.util.List;

@Repository
public interface ScenarioActionRepository extends JpaRepository<ScenarioAction, ScenarioActionId> {
    List<ScenarioAction> findByScenario(Scenario scenario);
    void deleteByScenario(Scenario scenario);
}