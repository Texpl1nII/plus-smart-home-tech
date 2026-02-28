package ru.yandex.practicum.commerce.warehouse.client;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.commerce.client.OrderClient;
import ru.yandex.practicum.commerce.client.ShoppingCartClient;

@Component
public class WarehouseClients {
    private final OrderClient orderClient;
    private final ShoppingCartClient shoppingCartClient;

    public WarehouseClients(OrderClient orderClient, ShoppingCartClient shoppingCartClient) {
        this.orderClient = orderClient;
        this.shoppingCartClient = shoppingCartClient;
    }

    public OrderClient getOrderClient() {
        return orderClient;
    }

    public ShoppingCartClient getShoppingCartClient() {
        return shoppingCartClient;
    }
}