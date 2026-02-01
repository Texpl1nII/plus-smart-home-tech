package ru.yandex.practicum.telemetry.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.telemetry.analyzer.entity.Scenario;
import ru.yandex.practicum.telemetry.analyzer.entity.ScenarioCondition;
import ru.yandex.practicum.telemetry.analyzer.entity.ScenarioConditionId;

import java.util.List;

@Repository
public interface ScenarioConditionRepository extends JpaRepository<ScenarioCondition, ScenarioConditionId> {
    List<ScenarioCondition> findByScenario(Scenario scenario);
    void deleteByScenario(Scenario scenario);
}
