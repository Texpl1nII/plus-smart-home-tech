package ru.yandex.practicum.commerce.warehouse;

import lombok.Builder;
import lombok.Data;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.util.UUID;

@Data
@Builder
public class AddProductToWarehouseRequest {

    private UUID productId;  // опционально, если товар уже есть

    @NotNull(message = "Количество обязательно")
    @Positive(message = "Количество должно быть положительным")
    private Long quantity;  // В спецификации int64
}