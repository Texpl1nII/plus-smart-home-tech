package ru.yandex.practicum.commerce.dto;

import lombok.Builder;
import lombok.Data;

import java.util.UUID;

@Data
@Builder
public class WarehouseProductDto {
    private UUID productId;
    private Integer quantity;
    private Double width;
    private Double height;
    private Double depth;
    private Double weight;
    private Boolean fragile;
}
