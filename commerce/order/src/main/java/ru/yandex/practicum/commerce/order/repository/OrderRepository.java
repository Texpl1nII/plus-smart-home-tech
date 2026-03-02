package ru.yandex.practicum.commerce.order.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.commerce.order.model.Order;
import ru.yandex.practicum.commerce.dto.enums.OrderState;

import java.util.List;
import java.util.UUID;

@Repository
public interface OrderRepository extends JpaRepository<Order, UUID> {
    List<Order> findAllByState(OrderState state);
    List<Order> findAllByShoppingCartId(UUID shoppingCartId);
    List<Order> findAllByShoppingCartIdIn(List<String> shoppingCartIds);
}