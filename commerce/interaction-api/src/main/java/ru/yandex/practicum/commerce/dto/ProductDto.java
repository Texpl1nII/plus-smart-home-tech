package ru.yandex.practicum.commerce.dto;

import lombok.Builder;
import lombok.Data;
import ru.yandex.practicum.commerce.dto.enums.ProductCategory;
import ru.yandex.practicum.commerce.dto.enums.ProductStatus;
import ru.yandex.practicum.commerce.dto.enums.AvailabilityStatus;

import java.util.UUID;

@Data
@Builder
public class ProductDto {
    private UUID productId;
    private String productName;
    private String description;
    private ProductCategory category;
    private Double price;
    private ProductStatus status;
    private AvailabilityStatus availability;
    private String imageUrl;
}
