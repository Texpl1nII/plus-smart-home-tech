package ru.yandex.practicum.commerce.delivery.client;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.commerce.client.OrderClient;
import ru.yandex.practicum.commerce.client.WarehouseClient;

@Component
public class DeliveryClients {
    private final OrderClient orderClient;
    private final WarehouseClient warehouseClient;

    public DeliveryClients(OrderClient orderClient, WarehouseClient warehouseClient) {
        this.orderClient = orderClient;
        this.warehouseClient = warehouseClient;
    }

    public OrderClient getOrderClient() {
        return orderClient;
    }

    public WarehouseClient getWarehouseClient() {
        return warehouseClient;
    }
}
