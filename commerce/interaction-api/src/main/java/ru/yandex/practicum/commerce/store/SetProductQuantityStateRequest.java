package ru.yandex.practicum.commerce.store;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.commerce.dto.enums.AvailabilityStatus;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SetProductQuantityStateRequest {

    @NotNull(message = "ID товара обязателен")
    @JsonProperty("productId")
    private UUID productId;

    @NotNull(message = "Статус количества обязателен")
    @JsonProperty("quantityState")
    private AvailabilityStatus quantityState;
}