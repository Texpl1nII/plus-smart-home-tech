package ru.yandex.practicum.commerce.payment.service;

import ru.yandex.practicum.commerce.dto.payment.PaymentDto;
import ru.yandex.practicum.commerce.dto.payment.PaymentRequest;

import java.math.BigDecimal;
import java.util.UUID;

public interface PaymentService {

    PaymentDto createPayment(PaymentRequest request);

    void paymentSuccess(UUID paymentId);

    void paymentFailed(UUID paymentId);

    BigDecimal calculateProductsCost(UUID orderId);

    BigDecimal calculateTotalCost(UUID orderId);
}
