package ru.yandex.practicum.commerce.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.delivery.DeliveryDto;
import java.util.UUID;

@FeignClient(name = "delivery", path = "/api/v1/delivery")
public interface DeliveryClient {

    @PostMapping
    DeliveryDto planDelivery(@RequestBody DeliveryDto deliveryDto);

    @PostMapping("/cost")
    Double calculateDeliveryCost(@RequestBody DeliveryDto deliveryDto);

    @PostMapping("/{deliveryId}/success")
    void deliverySuccess(@PathVariable("deliveryId") UUID deliveryId);

    @PostMapping("/{deliveryId}/failed")
    void deliveryFailed(@PathVariable("deliveryId") UUID deliveryId);
}
