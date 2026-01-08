package ru.yandex.practicum.telemetry.collector.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import ru.yandex.practicum.telemetry.collector.dto.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.collector.service.KafkaProducerService;

@Slf4j
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class SensorController {

    private final KafkaProducerService kafkaProducerService;

    @PostMapping("/sensors")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void collectSensorEvent(@Valid @RequestBody SensorEvent event) {
        log.info("Received sensor event: {}", event);
        kafkaProducerService.sendSensorEvent(event);
    }
}
