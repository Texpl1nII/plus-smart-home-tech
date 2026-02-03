package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.model.ScenarioAction;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioActionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;

import java.util.List;

@Slf4j
@Service
@Primary  // ← Сделайте его основным!
@RequiredArgsConstructor
public class SnapshotHandler {

    private final ScenarioRepository scenarioRepository;
    private final ScenarioActionRepository scenarioActionRepository;
    private final HubRouterClient hubRouterClient;

    @Transactional(readOnly = true)
    public void handle(SensorsSnapshotAvro sensorsSnapshotAvro) {
        String hubId = sensorsSnapshotAvro.getHubId();

        log.info("=== DEBUG SNAPSHOT HANDLER ===");
        log.info("Hub: {}", hubId);

        // Получаем все сценарии для хаба
        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);
        log.info("Found {} scenarios for hub {}", scenarios.size(), hubId);

        if (scenarios.isEmpty()) {
            log.error("❌ NO SCENARIOS FOUND! Check ScenarioAddedEventHandler");
            return;
        }

        // ВЫПОЛНЯЕМ ВСЕ ДЕЙСТВИЯ БЕЗ ПРОВЕРКИ УСЛОВИЙ
        for (Scenario scenario : scenarios) {
            log.info("=== EXECUTING SCENARIO WITHOUT CHECK: '{}' ===", scenario.getName());

            List<ScenarioAction> actions = scenarioActionRepository.findByScenario(scenario);
            log.info("Found {} actions for scenario '{}'", actions.size(), scenario.getName());

            for (ScenarioAction action : actions) {
                log.info("🚀 SENDING ACTION: sensor={}, type={}, value={}",
                        action.getSensor().getId(),
                        action.getAction().getType(),
                        action.getAction().getValue());

                try {
                    hubRouterClient.sendDeviceRequest(action);
                    log.info("✅ Action sent successfully!");
                } catch (Exception e) {
                    log.error("❌ Failed to send action: {}", e.getMessage());
                }
            }
        }

        log.info("=== DEBUG SNAPSHOT HANDLER END ===");
    }
}
