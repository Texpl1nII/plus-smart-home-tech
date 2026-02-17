package ru.yandex.practicum.commerce.warehouse;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class BookedProductsDto {

    private Double deliveryWeight;
    private Double deliveryVolume;
    private Boolean fragile;
}
