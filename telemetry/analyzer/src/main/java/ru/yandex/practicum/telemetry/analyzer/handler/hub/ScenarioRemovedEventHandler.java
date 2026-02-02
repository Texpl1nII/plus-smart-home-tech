package ru.yandex.practicum.telemetry.analyzer.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;  // ← ДОБАВЬТЕ!
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.telemetry.analyzer.handler.HubEventHandler;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioActionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;

import java.util.Optional;

@Slf4j  // ← ДОБАВЬТЕ!
@Component
@RequiredArgsConstructor
public class ScenarioRemovedEventHandler implements HubEventHandler {

    private final ScenarioRepository scenarioRepository;
    private final ScenarioActionRepository scenarioActionRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;

    @Override
    public String getEventType() {
        return ScenarioRemovedEventAvro.class.getSimpleName();
    }

    @Override
    public void handle(HubEventAvro event) {
        log.info("🔴 SCENARIO_REMOVED EVENT START");

        ScenarioRemovedEventAvro scenarioRemovedEventAvro = (ScenarioRemovedEventAvro) event.getPayload();
        log.info("Removing scenario: name={}, hub={}",
                scenarioRemovedEventAvro.getName(),
                event.getHubId());

        Optional<Scenario> scenarioOpt = scenarioRepository.findByHubIdAndName(
                event.getHubId(), scenarioRemovedEventAvro.getName());

        if (scenarioOpt.isPresent()) {
            Scenario scenario = scenarioOpt.get();
            scenarioActionRepository.deleteByScenario(scenario);
            scenarioConditionRepository.deleteByScenario(scenario);
            scenarioRepository.deleteByHubIdAndName(event.getHubId(), scenario.getName());
            log.info("✅ Scenario removed: {}", scenario.getName());
        } else {
            log.warn("Scenario not found: {}", scenarioRemovedEventAvro.getName());
        }

        log.info("🔴 SCENARIO_REMOVED EVENT END");
    }
}