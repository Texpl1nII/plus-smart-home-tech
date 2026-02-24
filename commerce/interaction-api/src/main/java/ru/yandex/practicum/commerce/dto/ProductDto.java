package ru.yandex.practicum.commerce.dto;

import lombok.Builder;
import lombok.Data;
import ru.yandex.practicum.commerce.dto.enums.ProductCategory;
import ru.yandex.practicum.commerce.dto.enums.ProductStatus;
import ru.yandex.practicum.commerce.dto.enums.AvailabilityStatus;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

@Data
@Builder
public class ProductDto {

    @JsonProperty("productId")
    private UUID productId;

    @JsonProperty("productName")
    private String productName;

    private String description;

    @JsonProperty("imageSrc")
    private String imageSrc;

    @JsonProperty("quantityState")
    private AvailabilityStatus quantityState;

    @JsonProperty("productState")
    private ProductStatus productState;

    @JsonProperty("productCategory")
    private ProductCategory productCategory;

    private Double price;
}
