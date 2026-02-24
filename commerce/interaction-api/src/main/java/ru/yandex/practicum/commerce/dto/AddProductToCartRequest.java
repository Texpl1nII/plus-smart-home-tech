package ru.yandex.practicum.commerce.dto;

import lombok.Builder;
import lombok.Data;

import jakarta.validation.constraints.*;
import java.util.UUID;

@Data
@Builder
public class AddProductToCartRequest {

    @NotNull(message = "ID товара обязателен")
    private UUID productId;

    @NotNull(message = "Количество товара обязательно")
    @Positive(message = "Количество должно быть положительным")
    private Integer quantity;

    private String username;
}