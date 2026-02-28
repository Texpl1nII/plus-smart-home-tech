package ru.yandex.practicum.commerce.order.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.order.client.OrderClients;
import ru.yandex.practicum.commerce.order.mapper.OrderMapper;
import ru.yandex.practicum.commerce.order.model.Order;
import ru.yandex.practicum.commerce.order.repository.OrderRepository;
import ru.yandex.practicum.commerce.order.service.OrderService;
import ru.yandex.practicum.commerce.dto.ShoppingCartDto;
import ru.yandex.practicum.commerce.dto.enums.OrderState;
import ru.yandex.practicum.commerce.dto.order.CreateOrderRequest;
import ru.yandex.practicum.commerce.dto.order.OrderDto;

import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;
    private final OrderMapper orderMapper;
    private final OrderClients clients;

    @Override
    @Transactional
    public OrderDto createOrder(CreateOrderRequest request) {
        log.info("Creating order from shopping cart: {}", request.getShoppingCartId());

        ShoppingCartDto cart = clients.getShoppingCartClient()
                .getShoppingCart(String.valueOf(request.getShoppingCartId()));

        Order order = Order.builder()
                .shoppingCartId(request.getShoppingCartId())
                .products(cart.getProducts())
                .state(OrderState.NEW)
                .build();

        order = orderRepository.save(order);
        log.info("Order created with id: {}", order.getOrderId());

        return orderMapper.toDto(order);
    }

    @Override
    public OrderDto getOrder(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new RuntimeException("Order not found: " + orderId));
        return orderMapper.toDto(order);
    }

    @Override
    public List<OrderDto> getUserOrders(UUID userId) {
        // TODO: получить корзины пользователя и найти заказы
        return List.of();
    }

    @Override
    @Transactional
    public void paymentSuccess(UUID orderId) {
        Order order = getOrderEntity(orderId);
        order.setState(OrderState.PAID);
        orderRepository.save(order);
        log.info("Order {} paid successfully", orderId);
    }

    @Override
    @Transactional
    public void paymentFailed(UUID orderId) {
        Order order = getOrderEntity(orderId);
        order.setState(OrderState.PAYMENT_FAILED);
        orderRepository.save(order);
        log.info("Payment failed for order {}", orderId);
    }

    @Override
    @Transactional
    public void deliverySuccess(UUID orderId) {
        Order order = getOrderEntity(orderId);
        order.setState(OrderState.DELIVERED);
        orderRepository.save(order);
        log.info("Order {} delivered successfully", orderId);
    }

    @Override
    @Transactional
    public void deliveryFailed(UUID orderId) {
        Order order = getOrderEntity(orderId);
        order.setState(OrderState.DELIVERY_FAILED);
        orderRepository.save(order);
        log.info("Delivery failed for order {}", orderId);
    }

    @Override
    @Transactional
    public void assemblySuccess(UUID orderId) {
        Order order = getOrderEntity(orderId);
        order.setState(OrderState.ASSEMBLED);
        orderRepository.save(order);
        log.info("Order {} assembled successfully", orderId);
    }

    @Override
    @Transactional
    public void assemblyFailed(UUID orderId) {
        Order order = getOrderEntity(orderId);
        order.setState(OrderState.ASSEMBLY_FAILED);
        orderRepository.save(order);
        log.info("Assembly failed for order {}", orderId);
    }

    @Override
    @Transactional
    public void returnOrder(UUID orderId) {
        Order order = getOrderEntity(orderId);
        order.setState(OrderState.PRODUCT_RETURNED);
        orderRepository.save(order);
        log.info("Products returned for order {}", orderId);
    }

    @Override
    @Transactional
    public void cancelOrder(UUID orderId) {
        Order order = getOrderEntity(orderId);
        order.setState(OrderState.CANCELED);
        orderRepository.save(order);
        log.info("Order {} cancelled", orderId);
    }

    private Order getOrderEntity(UUID orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new RuntimeException("Order not found: " + orderId));
    }
}