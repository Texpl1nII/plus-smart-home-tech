package ru.yandex.practicum.commerce.dto;

import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.UUID;

@Data
@Builder
public class ProductAvailabilityResponse {
    private boolean available;
    private List<UUID> unavailableProducts;
}
