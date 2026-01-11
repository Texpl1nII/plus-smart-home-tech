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
        log.info("Body content:\n{}", rawJson);

        try {
            JsonNode node = objectMapper.readTree(rawJson);
            log.info("Parsed JSON structure:\n{}", node.toPrettyString());

            if (node.has("id")) {
                log.info("Field 'id': {}", node.get("id"));
            }
            if (node.has("hub_id")) {
                log.info("Field 'hub_id': {}", node.get("hub_id"));
            }
            if (node.has("type")) {
                log.info("Field 'type': {}", node.get("type"));
            }
        } catch (Exception e) {
            log.error("Failed to parse JSON: {}", e.getMessage(), e);
        }

        log.info("=== END DEBUG ===");
        return "OK - Check logs for details";
    }

    @PostMapping("/hubs/raw")
    public String debugHubRaw(@RequestBody String rawJson) {
        log.info("=== RAW HUB JSON DEBUG ===");
        log.info("Body content:\n{}", rawJson);

        try {
            JsonNode node = objectMapper.readTree(rawJson);
            log.info("Parsed JSON structure:\n{}", node.toPrettyString());

            // Проверяем поля
            if (node.has("hub_id")) {
                log.info("Field 'hub_id': {}", node.get("hub_id"));
            }
            if (node.has("type")) {
                log.info("Field 'type': {}", node.get("type"));
            }
            if (node.has("device_type")) {
                log.info("Field 'device_type': {}", node.get("device_type"));
            }
            if (node.has("id")) {
                log.info("Field 'id': {}", node.get("id"));
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
