package ru.yandex.practicum.commerce.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.order.CreateOrderRequest;
import ru.yandex.practicum.commerce.dto.order.OrderDto;

import java.util.UUID;

@FeignClient(name = "order", path = "/api/v1/order")
public interface OrderClient {

    @PostMapping
    OrderDto createOrder(@RequestBody CreateOrderRequest request);

    @GetMapping("/{orderId}")
    OrderDto getOrder(@PathVariable UUID orderId);

    @PostMapping("/{orderId}/payment/success")
    void paymentSuccess(@PathVariable UUID orderId);

    @PostMapping("/{orderId}/payment/failed")
    void paymentFailed(@PathVariable UUID orderId);

    @PostMapping("/{orderId}/delivery/success")
    void deliverySuccess(@PathVariable UUID orderId);

    @PostMapping("/{orderId}/delivery/failed")
    void deliveryFailed(@PathVariable UUID orderId);

    @PostMapping("/{orderId}/assembly/success")
    void assemblySuccess(@PathVariable UUID orderId);

    @PostMapping("/{orderId}/assembly/failed")
    void assemblyFailed(@PathVariable UUID orderId);

    @PostMapping("/{orderId}/return")
    void returnOrder(@PathVariable UUID orderId);
}