package ru.yandex.practicum.telemetry.collector.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.telemetry.collector.dto.hub.HubEvent;
import ru.yandex.practicum.telemetry.collector.dto.hub.HubEventType;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEventType;
import ru.yandex.practicum.telemetry.collector.service.hub.HubEventHandler;
import ru.yandex.practicum.telemetry.collector.service.sensor.SensorEventHandler;

import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Validated
@RestController
@RequestMapping(path = "/events", consumes = MediaType.APPLICATION_JSON_VALUE)
public class EventController {
    private final Map<SensorEventType, SensorEventHandler> sensorEventHandlers;
    private final Map<HubEventType, HubEventHandler> hubEventHandlers;

    public EventController(List<SensorEventHandler> sensorEventHandlerList,
                           List<HubEventHandler> hubEventHandlerList) {
        this.sensorEventHandlers = sensorEventHandlerList.stream()
                .collect(Collectors.toMap(SensorEventHandler::getMessageType, Function.identity()));
        this.hubEventHandlers = hubEventHandlerList.stream()
                .collect(Collectors.toMap(HubEventHandler::getMessageType, Function.identity()));
    }

    @PostMapping("/sensors")
    public void collectSensorEvent(@Valid @RequestBody SensorEvent request) {
        log.info("Received sensor event: type={}, id={}, hubId={}",
                request.getType(), request.getId(), request.getHubId());

        if (sensorEventHandlers.containsKey(request.getType())) {
            sensorEventHandlers.get(request.getType()).handle(request);
        } else {
            throw new IllegalArgumentException("Не найден обработчик для события " + request.getType());
        }
    }

    @PostMapping("/hubs")
    public ResponseEntity<?> collectHubEvent(@Valid @RequestBody HubEvent request) {
        log.info("Received hub event: type={}, hubId={}", request.getType(), request.getHubId());

        try {
            if (hubEventHandlers.containsKey(request.getType())) {
                hubEventHandlers.get(request.getType()).handle(request);
                return ResponseEntity.ok().build();
            } else {
                log.error("No handler found for event type: {}", request.getType());
                return ResponseEntity.badRequest()
                        .body(Map.of("error", "Не найден обработчик для события " + request.getType()));
            }
        } catch (Exception e) {
            log.error("Error processing hub event: type={}, hubId={}",
                    request.getType(), request.getHubId(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Internal server error",
                            "message", e.getMessage(),
                            "exception", e.getClass().getName()));
        }
    }
}