package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.analyzer.model.*;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioActionRepository;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotHandler {

    private final ScenarioRepository scenarioRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;
    private final ScenarioActionRepository scenarioActionRepository;
    private final HubRouterClient hubRouterClient;

    @Transactional(readOnly = true)
    public void handle(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId();
        Map<String, SensorStateAvro> sensorStates = snapshot.getSensorsState();

        log.info("=== PROCESSING SNAPSHOT ===");
        log.info("Hub: {}", hubId);
        log.info("Sensors in snapshot: {}", sensorStates.size());

        // 1. Получаем все сценарии для хаба
        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);
        log.info("Found {} scenarios for hub {}", scenarios.size(), hubId);

        if (scenarios.isEmpty()) {
            log.info("No scenarios found for hub {}", hubId);
            return;
        }

        for (Scenario scenario : scenarios) {
            log.info("Checking scenario: '{}'", scenario.getName());

            // 2. Проверяем все условия сценария
            List<ScenarioCondition> conditions = scenarioConditionRepository.findByScenario(scenario);

            if (conditions.isEmpty()) {
                log.warn("Scenario '{}' has no conditions", scenario.getName());
                continue;
            }

            boolean allConditionsMet = checkAllConditions(conditions, sensorStates);

            if (allConditionsMet) {
                log.info("✅ ALL CONDITIONS MET for scenario '{}'", scenario.getName());
                // 3. Выполняем действия сценария
                executeActions(scenario);
            } else {
                log.info("❌ Conditions NOT met for scenario '{}'", scenario.getName());
            }
        }

        log.info("=== SNAPSHOT PROCESSING COMPLETE ===");
    }

    private boolean checkAllConditions(List<ScenarioCondition> conditions,
                                       Map<String, SensorStateAvro> sensorStates) {
        for (ScenarioCondition scenarioCondition : conditions) {
            String sensorId = scenarioCondition.getSensor().getId();
            Condition condition = scenarioCondition.getCondition();

            // Получаем состояние сенсора из снапшота
            SensorStateAvro sensorState = sensorStates.get(sensorId);
            if (sensorState == null) {
                log.warn("Sensor {} not found in snapshot", sensorId);
                return false;
            }

            // Получаем значение сенсора
            Integer sensorValue = extractSensorValue(sensorState.getData(), condition.getType());
            if (sensorValue == null) {
                log.warn("Cannot extract value for sensor {} type {}", sensorId, condition.getType());
                return false;
            }

            // Проверяем условие
            if (!checkCondition(condition, sensorValue)) {
                log.debug("Condition NOT met: sensor={}, type={}, value={}, condition={} {} {}",
                        sensorId, condition.getType(), sensorValue,
                        condition.getOperation(), condition.getValue());
                return false;
            }

            log.debug("Condition met: sensor={}, type={}, value={}, condition={} {} {}",
                    sensorId, condition.getType(), sensorValue,
                    condition.getOperation(), condition.getValue());
        }

        return true;
    }

    private Integer extractSensorValue(Object sensorData, ConditionTypeAvro type) {
        try {
            switch (type) {
                case TEMPERATURE:
                    if (sensorData instanceof ClimateSensorAvro) {
                        return ((ClimateSensorAvro) sensorData).getTemperatureC();
                    } else if (sensorData instanceof TemperatureSensorAvro) {
                        return ((TemperatureSensorAvro) sensorData).getTemperatureC();
                    }
                    break;

                case HUMIDITY:
                    if (sensorData instanceof ClimateSensorAvro) {
                        return ((ClimateSensorAvro) sensorData).getHumidity();
                    }
                    break;

                case CO2LEVEL:
                    if (sensorData instanceof ClimateSensorAvro) {
                        return ((ClimateSensorAvro) sensorData).getCo2Level();
                    }
                    break;

                case LUMINOSITY:
                    if (sensorData instanceof LightSensorAvro) {
                        return ((LightSensorAvro) sensorData).getLuminosity();
                    }
                    break;

                case MOTION:
                    if (sensorData instanceof MotionSensorAvro) {
                        return ((MotionSensorAvro) sensorData).getMotion() ? 1 : 0;
                    }
                    break;

                case SWITCH:
                    if (sensorData instanceof SwitchSensorAvro) {
                        return ((SwitchSensorAvro) sensorData).getState() ? 1 : 0;
                    }
                    break;
            }
        } catch (Exception e) {
            log.error("Error extracting sensor value for type {}: {}", type, e.getMessage());
        }

        return null;
    }

    private boolean checkCondition(Condition condition, Integer sensorValue) {
        if (sensorValue == null || condition.getValue() == null) {
            return false;
        }

        switch (condition.getOperation()) {
            case EQUALS:
                return sensorValue.equals(condition.getValue());
            case GREATER_THAN:
                return sensorValue > condition.getValue();
            case LOWER_THAN:
                return sensorValue < condition.getValue();
            default:
                log.warn("Unknown operation: {}", condition.getOperation());
                return false;
        }
    }

    private void executeActions(Scenario scenario) {
        List<ScenarioAction> actions = scenarioActionRepository.findByScenario(scenario);
        log.info("Executing {} actions for scenario '{}'", actions.size(), scenario.getName());

        for (ScenarioAction action : actions) {
            try {
                log.info("Sending action: sensor={}, type={}, value={}",
                        action.getSensor().getId(),
                        action.getAction().getType(),
                        action.getAction().getValue());

                hubRouterClient.sendDeviceRequest(action);
                log.info("✅ Action sent successfully");
            } catch (Exception e) {
                log.error("❌ Failed to send action: {}", e.getMessage());
            }
        }
    }
}
