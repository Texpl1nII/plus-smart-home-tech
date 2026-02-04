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

        log.info("🚨🚨🚨 SNAPSHOT HANDLER CALLED 🚨🚨🚨");
        log.info("Hub: {}, Sensors count: {}", hubId, sensorStates.size());
        log.info("Available sensors: {}", sensorStates.keySet());

        // 1. Получаем все сценарии для хаба
        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);
        log.info("Found {} scenarios for hub {}", scenarios.size(), hubId);

        if (scenarios.isEmpty()) {
            log.warn("❌ No scenarios found for hub {}", hubId);
            return;
        }

        for (Scenario scenario : scenarios) {
            log.info("=== CHECKING SCENARIO: '{}' (ID: {}) ===",
                    scenario.getName(), scenario.getId());

            // 2. Проверяем все условия сценария
            List<ScenarioCondition> conditions = scenarioConditionRepository.findByScenario(scenario);

            if (conditions.isEmpty()) {
                log.warn("❌ Scenario '{}' has no conditions", scenario.getName());
                continue;
            }

            log.info("Scenario has {} conditions", conditions.size());
            boolean allConditionsMet = checkAllConditions(conditions, sensorStates);

            if (allConditionsMet) {
                log.info("✅✅✅ ALL CONDITIONS MET for scenario '{}'", scenario.getName());
                // 3. Выполняем действия сценария
                executeActions(scenario);
            } else {
                log.info("❌ Some conditions NOT met for scenario '{}'", scenario.getName());
            }
        }

        log.info("=== SNAPSHOT PROCESSING COMPLETE ===");
    }

    private boolean checkAllConditions(List<ScenarioCondition> conditions,
                                       Map<String, SensorStateAvro> sensorStates) {
        log.info("🔍 Checking {} conditions", conditions.size());

        for (ScenarioCondition scenarioCondition : conditions) {
            String sensorId = scenarioCondition.getSensor().getId();
            Condition condition = scenarioCondition.getCondition();

            log.info("🔍 Checking condition: sensor={}, type={}, operation={}, target_value={}",
                    sensorId, condition.getType(), condition.getOperation(), condition.getValue());

            // Получаем состояние сенсора из снапшота
            SensorStateAvro sensorState = sensorStates.get(sensorId);
            if (sensorState == null) {
                log.error("❌❌❌ Sensor {} NOT FOUND in snapshot!", sensorId);
                log.info("Available sensors: {}", sensorStates.keySet());
                return false;
            }

            // Получаем значение сенсора
            Integer sensorValue = extractSensorValue(sensorState.getData(), condition.getType());
            log.info("Sensor {} current value: {}", sensorId, sensorValue);

            if (sensorValue == null) {
                log.error("❌❌❌ Cannot extract value for sensor {} type {}", sensorId, condition.getType());
                log.info("Sensor data class: {}", sensorState.getData().getClass().getSimpleName());
                return false;
            }

            // Проверяем условие
            boolean conditionMet = checkCondition(condition, sensorValue);
            log.info("Condition check: {} {} {} = {}",
                    sensorValue, condition.getOperation(), condition.getValue(), conditionMet);

            if (!conditionMet) {
                log.info("❌ Condition NOT met for sensor {}", sensorId);
                return false;
            }

            log.info("✅ Condition met for sensor {}", sensorId);
        }

        log.info("✅✅✅ ALL CONDITIONS MET");
        return true;
    }

    private Integer extractSensorValue(Object sensorData, ConditionTypeAvro type) {
        log.info("🔧 Extracting {} value from {} data",
                type, sensorData.getClass().getSimpleName());

        if (sensorData == null) {
            log.error("❌ Sensor data is null for type {}", type);
            return null;
        }

        try {
            switch (type) {
                case TEMPERATURE:
                    if (sensorData instanceof ClimateSensorAvro climateSensor) {
                        int temp = climateSensor.getTemperatureC();
                        log.info("🔧 Climate sensor temperature: {}°C", temp);
                        return temp;
                    } else if (sensorData instanceof TemperatureSensorAvro tempSensor) {
                        int temp = tempSensor.getTemperatureC();
                        log.info("🔧 Temperature sensor: {}°C", temp);
                        return temp;
                    }
                    log.warn("⚠️ Temperature expected but got: {}", sensorData.getClass().getSimpleName());
                    break;

                case HUMIDITY:
                    if (sensorData instanceof ClimateSensorAvro climateSensor) {
                        int humidity = climateSensor.getHumidity();
                        log.info("🔧 Climate sensor humidity: {}%", humidity);
                        return humidity;
                    }
                    break;

                case CO2LEVEL:
                    if (sensorData instanceof ClimateSensorAvro climateSensor) {
                        int co2 = climateSensor.getCo2Level();
                        log.info("🔧 Climate sensor CO2: {} ppm", co2);
                        return co2;
                    }
                    break;

                case LUMINOSITY:
                    if (sensorData instanceof LightSensorAvro lightSensor) {
                        int luminosity = lightSensor.getLuminosity();
                        log.info("🔧 Light sensor luminosity: {}", luminosity);
                        return luminosity;
                    }
                    break;

                case MOTION:
                    if (sensorData instanceof MotionSensorAvro motionSensor) {
                        boolean motion = motionSensor.getMotion();
                        int value = motion ? 1 : 0;
                        log.info("🔧 Motion sensor: {} -> {}", motion, value);
                        return value;
                    }
                    break;

                case SWITCH:
                    if (sensorData instanceof SwitchSensorAvro switchSensor) {
                        boolean state = switchSensor.getState();
                        int value = state ? 1 : 0;
                        log.info("🔧 Switch sensor: {} -> {}", state, value);
                        return value;
                    }
                    break;
            }
        } catch (Exception e) {
            log.error("❌ Error extracting sensor value for type {}: {}", type, e.getMessage(), e);
        }

        log.error("❌ Cannot extract {} value from {}", type, sensorData.getClass().getSimpleName());
        return null;
    }

    private boolean checkCondition(Condition condition, Integer sensorValue) {
        log.info("🔧 Checking condition: sensorValue={}, operation={}, targetValue={}",
                sensorValue, condition.getOperation(), condition.getValue());

        if (sensorValue == null) {
            log.error("❌ Sensor value is null");
            return false;
        }

        if (condition.getValue() == null) {
            log.error("❌ Condition target value is null");
            return false;
        }

        switch (condition.getOperation()) {
            case EQUALS:
                boolean equalsResult = sensorValue.equals(condition.getValue());
                log.info("🔧 EQUALS check: {} == {} = {}",
                        sensorValue, condition.getValue(), equalsResult);
                return equalsResult;
            case GREATER_THAN:
                boolean greaterResult = sensorValue > condition.getValue();
                log.info("🔧 GREATER_THAN check: {} > {} = {}",
                        sensorValue, condition.getValue(), greaterResult);
                return greaterResult;
            case LOWER_THAN:
                boolean lowerResult = sensorValue < condition.getValue();
                log.info("🔧 LOWER_THAN check: {} < {} = {}",
                        sensorValue, condition.getValue(), lowerResult);
                return lowerResult;
            default:
                log.error("❌ Unknown operation: {}", condition.getOperation());
                return false;
        }
    }

    private void executeActions(Scenario scenario) {
        List<ScenarioAction> actions = scenarioActionRepository.findByScenario(scenario);
        log.info("🚀🚀🚀 EXECUTING {} ACTIONS for scenario '{}' 🚀🚀🚀",
                actions.size(), scenario.getName());

        if (actions.isEmpty()) {
            log.error("❌❌❌ NO ACTIONS FOUND for scenario '{}'!", scenario.getName());
            return;
        }

        for (ScenarioAction action : actions) {
            try {
                log.info("=== SENDING ACTION ===");
                log.info("Sensor: {}", action.getSensor().getId());
                log.info("Action type: {}", action.getAction().getType());
                log.info("Action value: {}", action.getAction().getValue());

                hubRouterClient.sendDeviceRequest(action);
                log.info("✅✅✅ Action sent SUCCESSFULLY");

            } catch (Exception e) {
                log.error("❌❌❌ Failed to send action: {}", e.getMessage(), e);
            }
        }
    }
}
