package ru.yandex.practicum.telemetry.collector.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
@RestController
@RequestMapping(path = "/events", consumes = MediaType.APPLICATION_JSON_VALUE)
public class EventController {
    private final Map<SensorEventType, SensorEventHandler> sensorEventHandlers;
    private final Map<HubEventType, HubEventHandler> hubEventHandlers;

    public EventController(List<SensorEventHandler> sensorEventHandlerList, List<HubEventHandler> hubEventHandlerList) {
        // Добавлена проверка на дубликаты
        this.sensorEventHandlers = sensorEventHandlerList.stream()
                .collect(Collectors.toMap(
                        SensorEventHandler::getMessageType,
                        Function.identity(),
                        (existing, replacement) -> {
                            throw new IllegalStateException(
                                    "Найдено несколько обработчиков для типа события: " + existing.getMessageType()
                            );
                        }
                ));
        this.hubEventHandlers = hubEventHandlerList.stream()
                .collect(Collectors.toMap(
                        HubEventHandler::getMessageType,
                        Function.identity(),
                        (existing, replacement) -> {
                            throw new IllegalStateException(
                                    "Найдено несколько обработчиков для типа события: " + existing.getMessageType()
                            );
                        }
                ));
    }

    @PostMapping("/sensors")
    public void collectSensorEvent(@Valid @RequestBody SensorEvent request) {
        if (sensorEventHandlers.containsKey(request.getType())) {
            sensorEventHandlers.get(request.getType()).handle(request);
        } else {
            throw new IllegalArgumentException("Не найден обработчик для события " + request.getType());
        }
    }

    @PostMapping("/hubs")
    public void collectHubEvent(@Valid @RequestBody HubEvent request) {
        if (hubEventHandlers.containsKey(request.getType())) {
            hubEventHandlers.get(request.getType()).handle(request);
        } else {
            throw new IllegalArgumentException("Не найден обработчик для события " + request.getType());
        }
    }
}