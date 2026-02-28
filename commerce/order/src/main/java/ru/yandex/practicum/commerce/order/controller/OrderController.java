package ru.yandex.practicum.commerce.order.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.order.service.OrderService;
import ru.yandex.practicum.commerce.dto.order.CreateOrderRequest;
import ru.yandex.practicum.commerce.dto.order.OrderDto;

import java.util.List;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/order")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public OrderDto createOrder(@RequestBody CreateOrderRequest request) {
        log.info("POST /api/v1/order - creating order from cart: {}", request.getShoppingCartId());
        return orderService.createOrder(request);
    }

    @GetMapping("/{orderId}")
    public OrderDto getOrder(@PathVariable UUID orderId) {
        log.info("GET /api/v1/order/{}", orderId);
        return orderService.getOrder(orderId);
    }

    @GetMapping("/user/{userId}")
    public List<OrderDto> getUserOrders(@PathVariable UUID userId) {
        log.info("GET /api/v1/order/user/{}", userId);
        return orderService.getUserOrders(userId);
    }

    @PostMapping("/{orderId}/payment/success")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void paymentSuccess(@PathVariable UUID orderId) {
        log.info("POST /api/v1/order/{}/payment/success", orderId);
        orderService.paymentSuccess(orderId);
    }

    @PostMapping("/{orderId}/payment/failed")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void paymentFailed(@PathVariable UUID orderId) {
        log.info("POST /api/v1/order/{}/payment/failed", orderId);
        orderService.paymentFailed(orderId);
    }

    @PostMapping("/{orderId}/delivery/success")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deliverySuccess(@PathVariable UUID orderId) {
        log.info("POST /api/v1/order/{}/delivery/success", orderId);
        orderService.deliverySuccess(orderId);
    }

    @PostMapping("/{orderId}/delivery/failed")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deliveryFailed(@PathVariable UUID orderId) {
        log.info("POST /api/v1/order/{}/delivery/failed", orderId);
        orderService.deliveryFailed(orderId);
    }

    @PostMapping("/{orderId}/assembly/success")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void assemblySuccess(@PathVariable UUID orderId) {
        log.info("POST /api/v1/order/{}/assembly/success", orderId);
        orderService.assemblySuccess(orderId);
    }

    @PostMapping("/{orderId}/assembly/failed")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void assemblyFailed(@PathVariable UUID orderId) {
        log.info("POST /api/v1/order/{}/assembly/failed", orderId);
        orderService.assemblyFailed(orderId);
    }

    @PostMapping("/{orderId}/return")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void returnOrder(@PathVariable UUID orderId) {
        log.info("POST /api/v1/order/{}/return", orderId);
        orderService.returnOrder(orderId);
    }

    @PostMapping("/{orderId}/cancel")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void cancelOrder(@PathVariable UUID orderId) {
        log.info("POST /api/v1/order/{}/cancel", orderId);
        orderService.cancelOrder(orderId);
    }
}
