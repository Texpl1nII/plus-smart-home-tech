package ru.yandex.practicum.commerce.store;

import lombok.Builder;
import lombok.Data;
import ru.yandex.practicum.commerce.dto.enums.AvailabilityStatus;

import jakarta.validation.constraints.NotNull;
import java.util.UUID;

@Data
@Builder
public class SetProductQuantityStateRequest {

    @NotNull(message = "ID товара обязателен")
    private UUID productId;

    @NotNull(message = "Статус количества обязателен")
    private AvailabilityStatus quantityState;
}