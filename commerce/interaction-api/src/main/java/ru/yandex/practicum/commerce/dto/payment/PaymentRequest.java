package ru.yandex.practicum.commerce.dto.payment;

import lombok.Data;
import java.util.UUID;

@Data
public class PaymentRequest {
    private UUID orderId;
}
