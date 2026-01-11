package ru.yandex.practicum.telemetry.collector.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.telemetry.collector.dto.hub.HubEvent;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEvent;

@Slf4j
@RestController
@RequestMapping("/debug")
@RequiredArgsConstructor
public class DebugController {

    private final ObjectMapper objectMapper;

    @PostConstruct
    public void init() {
        log.info("=== JACKSON CONFIGURATION ===");
        log.info("PropertyNamingStrategy: {}", objectMapper.getPropertyNamingStrategy());
        log.info("Deserialization Features: {}", objectMapper.getDeserializationConfig());
        log.info("Serialization Features: {}", objectMapper.getSerializationConfig());
    }

    @PostMapping("/sensor-raw")
    public String debugSensorRaw(@RequestBody String rawJson) {
        log.info("=== RAW SENSOR JSON DEBUG ===");
        log.info("Body content: {}", rawJson);

        try {
            JsonNode node = objectMapper.readTree(rawJson);
            log.info("Parsed JSON structure:\n{}", node.toPrettyString());

            // Проверяем все поля
            log.info("=== FIELD CHECK ===");
            node.fieldNames().forEachRemaining(field ->
                    log.info("Field '{}': {}", field, node.get(field))
            );

            // Специальная проверка для hub_id/hubId
            if (node.has("hub_id")) {
                log.info("✓ Found 'hub_id': {}", node.get("hub_id").asText());
            } else if (node.has("hubId")) {
                log.info("✓ Found 'hubId': {}", node.get("hubId").asText());
            } else {
                log.warn("✗ Missing both 'hub_id' and 'hubId' fields!");
            }

            // Пробуем парсить как SensorEvent
            try {
                SensorEvent event = objectMapper.readValue(rawJson, SensorEvent.class);
                log.info("✓ Successfully parsed as SensorEvent: {}", event);
                log.info("  - id: {}", event.getId());
                log.info("  - hubId: {}", event.getHubId());
                log.info("  - type: {}", event.getType());
            } catch (Exception e) {
                log.error("✗ Failed to parse as SensorEvent: {}", e.getMessage());
            }

        } catch (Exception e) {
            log.error("Failed to parse JSON: {}", e.getMessage(), e);
        }

        log.info("=== END DEBUG ===");
        return "OK - Check logs for details";
    }

    @PostMapping("/hub-raw")
    public String debugHubRaw(@RequestBody String rawJson) {
        log.info("=== RAW HUB JSON DEBUG ===");
        log.info("Body content: {}", rawJson);

        try {
            JsonNode node = objectMapper.readTree(rawJson);
            log.info("Parsed JSON structure:\n{}", node.toPrettyString());

            // Проверяем все поля
            log.info("=== FIELD CHECK ===");
            node.fieldNames().forEachRemaining(field ->
                    log.info("Field '{}': {}", field, node.get(field))
            );

            // Специальная проверка для hub_id/hubId
            if (node.has("hub_id")) {
                log.info("✓ Found 'hub_id': {}", node.get("hub_id").asText());
            } else if (node.has("hubId")) {
                log.info("✓ Found 'hubId': {}", node.get("hubId").asText());
            } else {
                log.warn("✗ Missing both 'hub_id' and 'hubId' fields!");
            }

            // Пробуем парсить как HubEvent
            try {
                HubEvent event = objectMapper.readValue(rawJson, HubEvent.class);
                log.info("✓ Successfully parsed as HubEvent: {}", event);
                log.info("  - hubId: {}", event.getHubId());
                log.info("  - type: {}", event.getType());
            } catch (Exception e) {
                log.error("✗ Failed to parse as HubEvent: {}", e.getMessage());
            }

        } catch (Exception e) {
            log.error("Failed to parse JSON: {}", e.getMessage(), e);
        }

        log.info("=== END DEBUG ===");
        return "OK - Check logs for details";
    }

    @GetMapping("/jackson-config")
    public String getJacksonConfig() {
        return String.format(
                "PropertyNamingStrategy: %s\n" +
                        "FailOnUnknownProperties: %s\n" +
                        "DefaultPropertyInclusion: %s",
                objectMapper.getPropertyNamingStrategy(),
                objectMapper.getDeserializationConfig().isEnabled(
                        com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES
                ),
                objectMapper.getSerializationConfig().getDefaultPropertyInclusion()
        );
    }
}
