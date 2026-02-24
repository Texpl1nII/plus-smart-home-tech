package ru.yandex.practicum.commerce.cart;

import lombok.Builder;
import lombok.Data;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.util.UUID;

@Data
@Builder
public class ChangeProductQuantityRequest {

    @NotNull(message = "ID товара обязателен")
    private UUID productId;

    @NotNull(message = "Новое количество обязательно")
    @Positive(message = "Количество должно быть положительным")
    private Long newQuantity;  // В спецификации int64
}