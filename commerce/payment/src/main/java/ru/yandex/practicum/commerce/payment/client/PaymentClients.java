package ru.yandex.practicum.commerce.payment.client;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.commerce.client.OrderClient;
import ru.yandex.practicum.commerce.client.StoreClient;

@Component
public class PaymentClients {
    private final OrderClient orderClient;
    private final StoreClient storeClient;

    public PaymentClients(OrderClient orderClient, StoreClient storeClient) {
        this.orderClient = orderClient;
        this.storeClient = storeClient;
    }

    public OrderClient getOrderClient() {
        return orderClient;
    }

    public StoreClient getStoreClient() {
        return storeClient;
    }
}
