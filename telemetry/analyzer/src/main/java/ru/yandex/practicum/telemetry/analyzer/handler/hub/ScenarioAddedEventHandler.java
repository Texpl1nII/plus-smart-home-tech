package ru.yandex.practicum.telemetry.analyzer.handler.hub;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.analyzer.handler.HubEventHandler;
import ru.yandex.practicum.telemetry.analyzer.model.*;
import ru.yandex.practicum.telemetry.analyzer.repository.*;

import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScenarioAddedEventHandler implements HubEventHandler {

    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;
    private final ScenarioActionRepository scenarioActionRepository;
    private final ScenarioConditionRepository scenarioConditionRepository;

    @Override
    public String getEventType() {
        return ScenarioAddedEventAvro.class.getSimpleName();
    }

    @Override
    public void handle(HubEventAvro event) {
        log.info("=== SCENARIO_ADDED EVENT START ===");
        log.info("Hub: {}", event.getHubId());

        ScenarioAddedEventAvro scenarioAddedEventAvro = (ScenarioAddedEventAvro) event.getPayload();
        log.info("Scenario name: {}, Conditions: {}, Actions: {}",
                scenarioAddedEventAvro.getName(),
                scenarioAddedEventAvro.getConditions().size(),
                scenarioAddedEventAvro.getActions().size());

        // Проверяем существование сценария
        Optional<Scenario> existingScenario = scenarioRepository.findByHubIdAndName(
                event.getHubId(), scenarioAddedEventAvro.getName());

        if (existingScenario.isPresent()) {
            log.info("Updating existing scenario: {}", scenarioAddedEventAvro.getName());
            Scenario prevScenario = existingScenario.get();
            scenarioActionRepository.deleteByScenario(prevScenario);
            scenarioConditionRepository.deleteByScenario(prevScenario);
            scenarioRepository.deleteByHubIdAndName(
                    prevScenario.getHubId(),
                    prevScenario.getName()
            );
        } else {
            log.info("Creating new scenario: {}", scenarioAddedEventAvro.getName());
        }

        Scenario scenarioToUpload = Scenario.builder()
                .name(scenarioAddedEventAvro.getName())
                .hubId(event.getHubId())
                .build();

        Scenario savedScenario = scenarioRepository.save(scenarioToUpload);
        log.info("✅ Scenario saved with ID: {}", savedScenario.getId());

        saveConditions(savedScenario, event, scenarioAddedEventAvro);
        saveActions(savedScenario, event, scenarioAddedEventAvro);

        log.info("=== SCENARIO_ADDED EVENT END ===");
    }

    private void saveConditions(Scenario scenario, HubEventAvro event, ScenarioAddedEventAvro avro) {
        log.info("🚨 SAVE CONDITIONS - DEBUG MODE 🚨");
        log.info("Scenario: {} (ID: {}), Hub: {}",
                scenario.getName(), scenario.getId(), scenario.getHubId());
        log.info("Total conditions to save: {}", avro.getConditions().size());

        for (ScenarioConditionAvro conditionAvro : avro.getConditions()) {
            log.info("📝 Processing condition for sensor: {}", conditionAvro.getSensorId());

            // ЛОГИРУЕМ СЫРЫЕ ДАННЫЕ ПЕРЕД ОБРАБОТКОЙ
            log.info("  Raw type: {} (class: {})",
                    conditionAvro.getType(), conditionAvro.getType().getClass().getName());
            log.info("  Raw operation: {} (class: {})",
                    conditionAvro.getOperation(), conditionAvro.getOperation().getClass().getName());
            log.info("  Raw value: {} (class: {})",
                    conditionAvro.getValue(),
                    conditionAvro.getValue() != null ?
                            conditionAvro.getValue().getClass().getName() : "null");

            // Ищем сенсор
            Sensor sensor = sensorRepository.findByIdAndHubId(
                            conditionAvro.getSensorId(), event.getHubId())
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Sensor not found: " + conditionAvro.getSensorId() +
                                    " for hub: " + event.getHubId()));

            Integer value = extractConditionValue(conditionAvro.getValue());

            // ПРЕОБРАЗУЕМ ТИП - УПРОЩЕННЫЙ ВАРИАНТ
            ConditionTypeAvro typeAvro = ConditionTypeAvro.valueOf(conditionAvro.getType().toString());
            ConditionOperationAvro operationAvro = ConditionOperationAvro.valueOf(conditionAvro.getOperation().toString());

            log.info("✅ Converted: sensor={}, type={}, operation={}, value={}",
                    sensor.getId(), typeAvro, operationAvro, value);

            Condition condition = conditionRepository.save(
                    Condition.builder()
                            .type(typeAvro)
                            .operation(operationAvro)
                            .value(value)
                            .build()
            );

            scenarioConditionRepository.save(
                    ScenarioCondition.builder()
                            .scenario(scenario)
                            .sensor(sensor)
                            .condition(condition)
                            .id(new ScenarioConditionId(
                                    scenario.getId(),
                                    sensor.getId(),
                                    condition.getId()
                            ))
                            .build()
            );

            log.info("✅ Condition saved with ID: {}", condition.getId());
        }
    }

    private Integer extractConditionValue(Object value) {
        if (value == null) {
            log.warn("⚠️ Condition value is null, defaulting to 0");
            return 0;
        }

        log.info("🔍 EXTRACTING VALUE: {} (class: {})",
                value, value.getClass().getName());

        try {
            // 1. Boolean - для SWITCH и MOTION сенсоров
            if (value instanceof Boolean) {
                boolean boolVal = (Boolean) value;
                log.info("🔍 Boolean detected: {} -> {}", boolVal, boolVal ? 1 : 0);
                return boolVal ? 1 : 0;
            }

            // 2. Integer - для числовых сенсоров
            if (value instanceof Integer) {
                log.info("🔍 Integer detected: {}", value);
                return (Integer) value;
            }

            // 3. Long - КРИТИЧЕСКО ВАЖНО! Avro может вернуть Long для boolean (0L или 1L)
            if (value instanceof Long) {
                long longVal = (Long) value;
                log.info("🔍 Long detected: {} -> {}", longVal, (int) longVal);

                // Если это boolean в виде Long
                if (longVal == 0L || longVal == 1L) {
                    return (int) longVal;
                }
                return (int) longVal;
            }

            // 4. Другие числовые типы
            if (value instanceof Number) {
                int numVal = ((Number) value).intValue();
                log.info("🔍 Number detected: {} -> {}", value, numVal);
                return numVal;
            }

            // 5. Строковое представление
            String strVal = value.toString().toLowerCase().trim();
            log.info("🔍 String detected: '{}'", strVal);

            if ("true".equals(strVal) || "1".equals(strVal)) {
                return 1;
            } else if ("false".equals(strVal) || "0".equals(strVal)) {
                return 0;
            } else {
                return Integer.parseInt(strVal);
            }

        } catch (Exception e) {
            log.error("❌ Cannot convert value to Integer: {} (type: {})",
                    value, value.getClass().getName(), e);
            return 0;
        }
    }

    private void saveActions(Scenario scenario, HubEventAvro event, ScenarioAddedEventAvro avro) {
        log.info("Saving {} actions...", avro.getActions().size());

        for (DeviceActionAvro actionAvro : avro.getActions()) {
            // Ищем сенсор
            Sensor sensor = sensorRepository.findByIdAndHubId(
                            actionAvro.getSensorId(), event.getHubId())
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Sensor not found: " + actionAvro.getSensorId() +
                                    " for hub: " + event.getHubId()));

            // ПРОСТОЕ ПРЕОБРАЗОВАНИЕ ТИПА
            ActionTypeAvro typeAvro = ActionTypeAvro.valueOf(actionAvro.getType().toString());

            // ОБРАБАТЫВАЕМ ЗНАЧЕНИЕ (может быть null)
            Integer actionValue = actionAvro.getValue() != null ? actionAvro.getValue() : 0;

            log.info("Saving action: sensor={}, type={}, value={}",
                    sensor.getId(), typeAvro, actionValue);

            Action action = actionRepository.save(
                    Action.builder()
                            .type(typeAvro)
                            .value(actionValue)
                            .build()
            );

            scenarioActionRepository.save(
                    ScenarioAction.builder()
                            .scenario(scenario)
                            .sensor(sensor)
                            .action(action)
                            .id(new ScenarioActionId(
                                    scenario.getId(),
                                    sensor.getId(),
                                    action.getId()
                            ))
                            .build()
            );

            log.info("✅ Action saved with ID: {}", action.getId());
        }
    }
}