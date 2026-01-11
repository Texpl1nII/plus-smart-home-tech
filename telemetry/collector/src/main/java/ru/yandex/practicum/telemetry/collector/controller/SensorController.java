package ru.yandex.practicum.telemetry.collector.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.collector.service.KafkaProducerService;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/events")
@RequiredArgsConstructor
public class SensorController {

    private final KafkaProducerService kafkaProducerService;

    @PostMapping("/sensors")
    public ResponseEntity<?> collectSensorEvent(@Valid @RequestBody SensorEvent event) {
        log.info("Received sensor event: type={}, id={}, hubId={}",
                event.getType(), event.getId(), event.getHubId());

        try {
            // Отправляем в Kafka
            kafkaProducerService.sendSensorEvent(event);

            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Sensor event processed successfully");
            response.put("timestamp", Instant.now().toString());

            return ResponseEntity.ok(response);

        } catch (IllegalArgumentException e) {
            log.error("Validation error in sensor event: {}", e.getMessage());

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Validation failed");
            errorResponse.put("message", e.getMessage());
            errorResponse.put("timestamp", Instant.now().toString());
            errorResponse.put("status", HttpStatus.BAD_REQUEST.value());

            return ResponseEntity.badRequest().body(errorResponse);

        } catch (Exception e) {
            log.error("Error processing sensor event", e);

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Internal server error");
            errorResponse.put("message", "Failed to process sensor event");
            errorResponse.put("timestamp", Instant.now().toString());
            errorResponse.put("status", HttpStatus.INTERNAL_SERVER_ERROR.value());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
}
