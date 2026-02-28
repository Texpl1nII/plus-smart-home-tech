package ru.yandex.practicum.commerce.dto.order;

import lombok.Data;
import lombok.Builder;
import ru.yandex.practicum.commerce.dto.enums.OrderState;

import java.util.Map;
import java.util.UUID;

@Data
@Builder
public class OrderDto {
    private UUID orderId;
    private UUID shoppingCartId;
    private Map<UUID, Long> products; // productId -> quantity
    private OrderState state;
    private Double totalPrice;
    private Double productsPrice;
    private Double deliveryPrice;
    private Double volume;
    private Double weight;
    private Boolean fragile;
    private UUID deliveryId;
    private UUID paymentId;
}
