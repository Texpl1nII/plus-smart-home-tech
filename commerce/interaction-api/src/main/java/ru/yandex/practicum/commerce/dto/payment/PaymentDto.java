package ru.yandex.practicum.commerce.dto.payment;

import lombok.Data;
import lombok.Builder;
import ru.yandex.practicum.commerce.dto.enums.PaymentStatus;

import java.util.UUID;

@Data
@Builder
public class PaymentDto {
    private UUID paymentId;
    private UUID orderId;
    private Double productsTotal;
    private Double deliveryTotal;
    private Double total;
    private PaymentStatus status;
}