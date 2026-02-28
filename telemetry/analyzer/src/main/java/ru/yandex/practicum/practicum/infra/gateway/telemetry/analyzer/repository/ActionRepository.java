package ru.yandex.practicum.practicum.infra.gateway.telemetry.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.practicum.infra.gateway.telemetry.analyzer.model.Action;

@Repository
public interface ActionRepository extends JpaRepository<Action, Long> {
}
