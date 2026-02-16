package ru.yandex.practicum.commerce.dto;

import lombok.Builder;
import lombok.Data;

import java.util.Map;
import java.util.UUID;

@Data
@Builder
public class ShoppingCartDto {
    private String shoppingCartId;
    private String userId;
    private Map<UUID, Integer> products; // productId -> quantity
    private boolean active;
}