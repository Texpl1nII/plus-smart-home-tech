package ru.yandex.practicum.telemetry.analyzer.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.telemetry.analyzer.entity.Scenario;
import ru.yandex.practicum.telemetry.analyzer.handler.HubEventHandler;
import ru.yandex.practicum.telemetry.analyzer.repository.*;

import java.util.Optional;

@Slf4j
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
    @Transactional
    public void handle(HubEventAvro event) {
        ScenarioRemovedEventAvro scenarioEvent = (ScenarioRemovedEventAvro) event.getPayload();

        Optional<Scenario> scenario = scenarioRepository
                .findByHubIdAndName(event.getHubId(), scenarioEvent.getName());

        if (scenario.isPresent()) {
            Scenario scenarioToDelete = scenario.get();

            // Удаляем связи
            scenarioActionRepository.deleteByScenario(scenarioToDelete);
            scenarioConditionRepository.deleteByScenario(scenarioToDelete);

            // Удаляем сценарий
            scenarioRepository.delete(scenarioToDelete);

            log.info("Scenario removed: {} from hub: {}",
                    scenarioEvent.getName(), event.getHubId());
        } else {
            log.warn("Scenario not found for removal: {} in hub: {}",
                    scenarioEvent.getName(), event.getHubId());
        }
    }
}