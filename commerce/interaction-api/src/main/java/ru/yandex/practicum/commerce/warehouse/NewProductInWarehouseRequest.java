package ru.yandex.practicum.commerce.warehouse;

import lombok.Builder;
import lombok.Data;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.util.UUID;

@Data
@Builder
public class NewProductInWarehouseRequest {

    @NotNull(message = "ID товара обязателен")
    private UUID productId;

    private Boolean fragile;

    @NotNull(message = "Размеры товара обязательны")
    private DimensionDto dimension;

    @NotNull(message = "Вес товара обязателен")
    @Positive(message = "Вес должен быть положительным")
    private Double weight;
}
