package ru.yandex.practicum.commerce.warehouse;

import lombok.Data;
import java.util.UUID;

@Data
public class ShippedToDeliveryRequest {
    private UUID orderId;
    private UUID deliveryId;
}