package ru.yandex.practicum.commerce.delivery.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.delivery.service.DeliveryService;
import ru.yandex.practicum.commerce.dto.delivery.DeliveryDto;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/delivery")
@RequiredArgsConstructor
public class DeliveryController {

    private final DeliveryService deliveryService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public DeliveryDto planDelivery(@RequestBody DeliveryDto deliveryDto) {
        log.info("POST /api/v1/delivery - for order: {}", deliveryDto.getOrderId());
        return deliveryService.planDelivery(deliveryDto);
    }

    @PostMapping("/cost")
    public BigDecimal calculateDeliveryCost(@RequestBody DeliveryDto deliveryDto) {
        log.info("POST /api/v1/delivery/cost - for order: {}", deliveryDto.getOrderId());
        return deliveryService.calculateDeliveryCost(deliveryDto);
    }

    @PostMapping("/{deliveryId}/success")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deliverySuccess(@PathVariable UUID deliveryId) {
        log.info("POST /api/v1/delivery/{}/success", deliveryId);
        deliveryService.deliverySuccess(deliveryId);
    }

    @PostMapping("/{deliveryId}/failed")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deliveryFailed(@PathVariable UUID deliveryId) {
        log.info("POST /api/v1/delivery/{}/failed", deliveryId);
        deliveryService.deliveryFailed(deliveryId);
    }
}