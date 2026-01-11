package ru.yandex.practicum.telemetry.collector.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.telemetry.collector.dto.hub.HubEvent;
import ru.yandex.practicum.telemetry.collector.service.KafkaProducerService;

import jakarta.validation.Valid;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/telemetry")
public class HubController {

    private final KafkaProducerService kafkaProducerService;

    @PostMapping("/hubs")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void collectHubEvent(@Valid @RequestBody HubEvent event) {
        log.info("Received hub event: {}", event);
        kafkaProducerService.sendHubEvent(event);
    }
}