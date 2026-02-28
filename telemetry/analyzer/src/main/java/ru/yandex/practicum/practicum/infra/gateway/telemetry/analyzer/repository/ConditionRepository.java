package ru.yandex.practicum.practicum.infra.gateway.telemetry.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.practicum.infra.gateway.telemetry.analyzer.model.Condition;

@Repository
public interface ConditionRepository extends JpaRepository<Condition, Long> {
}