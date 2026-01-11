package ru.yandex.practicum.telemetry.collector.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/debug")
@RequiredArgsConstructor
public class DebugController {

    private final ObjectMapper objectMapper;

    @PostMapping("/sensors/raw")
    public String debugSensorRaw(@RequestBody String rawJson) {
        log.info("=== RAW SENSOR JSON DEBUG ===");
        log.info("Content-Type should be: application/json");
        log.info("Body length: {} chars", rawJson.length());
        log.info("Body content:\n{}", rawJson);

        try {
            JsonNode node = objectMapper.readTree(rawJson);
            log.info("Parsed JSON structure:\n{}", node.toPrettyString());
        } catch (Exception e) {
            log.error("Failed to parse JSON: {}", e.getMessage(), e);
        }

        log.info("=== END DEBUG ===");
        return "OK - Check logs for details";
    }

    @PostMapping("/hubs/raw")
    public String debugHubRaw(@RequestBody String rawJson) {
        log.info("=== RAW HUB JSON DEBUG ===");
        log.info("Body length: {} chars", rawJson.length());
        log.info("Body content:\n{}", rawJson);

        try {
            JsonNode node = objectMapper.readTree(rawJson);
            log.info("Parsed JSON structure:\n{}", node.toPrettyString());

            // Проверяем поля
            if (node.has("hub_id")) {
                log.info("Field 'hub_id' found: {}", node.get("hub_id"));
            }
            if (node.has("type")) {
                log.info("Field 'type' found: {}", node.get("type"));
            }
            if (node.has("payload")) {
                log.info("Field 'payload' found: {}", node.get("payload"));
            }
        } catch (Exception e) {
            log.error("Failed to parse JSON: {}", e.getMessage(), e);
        }

        log.info("=== END DEBUG ===");
        return "OK - Check logs for details";
    }

    @GetMapping("/health")
    public String health() {
        return "Collector is UP";
    }
}
