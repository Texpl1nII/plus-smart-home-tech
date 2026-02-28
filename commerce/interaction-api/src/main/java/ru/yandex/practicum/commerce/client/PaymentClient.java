package ru.yandex.practicum.commerce.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.payment.PaymentDto;
import ru.yandex.practicum.commerce.dto.payment.PaymentRequest;
import java.util.UUID;

@FeignClient(name = "payment", path = "/api/v1/payment")
public interface PaymentClient {

    @PostMapping("/calculate/products")
    Double calculateProductsCost(@RequestParam UUID orderId);

    @PostMapping("/calculate/total")
    Double calculateTotalCost(@RequestParam UUID orderId);

    @PostMapping
    PaymentDto createPayment(@RequestBody PaymentRequest request);

    @PostMapping("/{paymentId}/success")
    void paymentSuccess(@PathVariable("paymentId") UUID paymentId);

    @PostMapping("/{paymentId}/failed")
    void paymentFailed(@PathVariable("paymentId") UUID paymentId);
}