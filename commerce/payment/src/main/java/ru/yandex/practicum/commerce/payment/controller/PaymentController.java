package ru.yandex.practicum.commerce.payment.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.payment.PaymentDto;
import ru.yandex.practicum.commerce.dto.payment.PaymentRequest;
import ru.yandex.practicum.commerce.payment.service.PaymentService;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/payment")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;

    @PostMapping("/calculate/products")
    public BigDecimal calculateProductsCost(@RequestParam UUID orderId) {
        log.info("POST /api/v1/payment/calculate/products?orderId={}", orderId);
        return paymentService.calculateProductsCost(orderId);  // Убрали BigDecimal.valueOf()
    }

    @PostMapping("/calculate/total")
    public BigDecimal calculateTotalCost(@RequestParam UUID orderId) {
        log.info("POST /api/v1/payment/calculate/total?orderId={}", orderId);
        return paymentService.calculateTotalCost(orderId);  // Убрали BigDecimal.valueOf()
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public PaymentDto createPayment(@RequestBody PaymentRequest request) {
        log.info("POST /api/v1/payment - for order: {}", request.getOrderId());
        return paymentService.createPayment(request);
    }

    @PostMapping("/{paymentId}/success")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void paymentSuccess(@PathVariable UUID paymentId) {
        log.info("POST /api/v1/payment/{}/success", paymentId);
        paymentService.paymentSuccess(paymentId);
    }

    @PostMapping("/{paymentId}/failed")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void paymentFailed(@PathVariable UUID paymentId) {
        log.info("POST /api/v1/payment/{}/failed", paymentId);
        paymentService.paymentFailed(paymentId);
    }
}
