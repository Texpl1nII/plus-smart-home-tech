package ru.yandex.practicum.commerce.dto.order;

import lombok.Data;
import java.util.UUID;

@Data
public class CreateOrderRequest {
    private UUID shoppingCartId;
}