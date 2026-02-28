package ru.yandex.practicum.commerce.order.service;

import ru.yandex.practicum.commerce.dto.order.CreateOrderRequest;
import ru.yandex.practicum.commerce.dto.order.OrderDto;

import java.util.List;
import java.util.UUID;

public interface OrderService {
    OrderDto createOrder(CreateOrderRequest request);
    OrderDto getOrder(UUID orderId);
    List<OrderDto> getUserOrders(UUID userId);

    void paymentSuccess(UUID orderId);
    void paymentFailed(UUID orderId);

    void deliverySuccess(UUID orderId);
    void deliveryFailed(UUID orderId);

    void assemblySuccess(UUID orderId);
    void assemblyFailed(UUID orderId);

    void returnOrder(UUID orderId);
    void cancelOrder(UUID orderId);
}
