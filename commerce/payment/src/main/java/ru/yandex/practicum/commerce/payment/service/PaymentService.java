package ru.yandex.practicum.commerce.payment.service;

import ru.yandex.practicum.commerce.dto.payment.PaymentDto;
import ru.yandex.practicum.commerce.dto.payment.PaymentRequest;

import java.util.UUID;

public interface PaymentService {

    Double calculateProductsCost(UUID orderId);

    Double calculateTotalCost(UUID orderId);

    PaymentDto createPayment(PaymentRequest request);

    void paymentSuccess(UUID paymentId);

    void paymentFailed(UUID paymentId);
}
