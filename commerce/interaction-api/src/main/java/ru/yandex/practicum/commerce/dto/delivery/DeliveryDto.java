package ru.yandex.practicum.commerce.dto.delivery;

import lombok.Data;
import lombok.Builder;
import ru.yandex.practicum.commerce.dto.AddressDto;
import ru.yandex.practicum.commerce.dto.enums.DeliveryState;

import java.math.BigDecimal;  // добавить импорт
import java.util.UUID;

@Data
@Builder
public class DeliveryDto {
    private UUID deliveryId;
    private UUID orderId;
    private AddressDto fromAddress;
    private AddressDto toAddress;
    private Double volume;
    private Double weight;
    private Boolean fragile;
    private DeliveryState state;
    private BigDecimal deliveryCost;
}
