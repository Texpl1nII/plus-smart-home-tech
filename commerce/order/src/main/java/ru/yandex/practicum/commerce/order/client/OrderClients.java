package ru.yandex.practicum.commerce.order.client;


import org.springframework.stereotype.Component;
import ru.yandex.practicum.commerce.client.*;


@Component
public class OrderClients {
    private final PaymentClient paymentClient;
    private final DeliveryClient deliveryClient;
    private final WarehouseClient warehouseClient;
    private final StoreClient storeClient;
    private final ShoppingCartClient shoppingCartClient;

    public OrderClients(PaymentClient paymentClient,
                        DeliveryClient deliveryClient,
                        WarehouseClient warehouseClient,
                        StoreClient storeClient,
                        ShoppingCartClient shoppingCartClient) {
        this.paymentClient = paymentClient;
        this.deliveryClient = deliveryClient;
        this.warehouseClient = warehouseClient;
        this.storeClient = storeClient;
        this.shoppingCartClient = shoppingCartClient;
    }

    public PaymentClient getPaymentClient() {
        return paymentClient;
    }

    public DeliveryClient getDeliveryClient() {
        return deliveryClient;
    }

    public WarehouseClient getWarehouseClient() {
        return warehouseClient;
    }

    public StoreClient getStoreClient() {
        return storeClient;
    }

    public ShoppingCartClient getShoppingCartClient() {  // ДОБАВИТЬ
        return shoppingCartClient;
    }
}