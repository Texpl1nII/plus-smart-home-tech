package ru.yandex.practicum.telemetry.collector.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.collector.service.KafkaProducerService;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequiredArgsConstructor
public class SensorController {

    private final KafkaProducerService kafkaProducerService;

    @PostMapping("/sensors")
    public ResponseEntity<?> collectSensorEvent(@Valid @RequestBody SensorEvent event) {
        log.info("Received sensor event: {}", event);

        try {
            kafkaProducerService.sendSensorEvent(event);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Sensor event processed successfully");

            return ResponseEntity.ok(response);

        } catch (IllegalArgumentException e) {
            log.error("Validation error: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Validation failed");
            errorResponse.put("message", e.getMessage());
            errorResponse.put("timestamp", java.time.Instant.now().toString());
            errorResponse.put("status", 400);

            return ResponseEntity.badRequest().body(errorResponse);

        } catch (Exception e) {
            log.error("Error processing sensor event", e);

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Internal server error");
            errorResponse.put("message", e.getMessage());
            errorResponse.put("timestamp", java.time.Instant.now().toString());
            errorResponse.put("status", 500);

            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
}
