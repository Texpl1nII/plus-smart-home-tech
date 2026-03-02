package ru.yandex.practicum.commerce.payment.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.ProductDto;
import ru.yandex.practicum.commerce.dto.enums.PaymentStatus;
import ru.yandex.practicum.commerce.dto.order.OrderDto;
import ru.yandex.practicum.commerce.dto.payment.PaymentDto;
import ru.yandex.practicum.commerce.dto.payment.PaymentRequest;
import ru.yandex.practicum.commerce.payment.client.PaymentClients;
import ru.yandex.practicum.commerce.payment.mapper.PaymentMapper;
import ru.yandex.practicum.commerce.payment.model.Payment;
import ru.yandex.practicum.commerce.payment.repository.PaymentRepository;
import ru.yandex.practicum.commerce.payment.service.PaymentService;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentServiceImpl implements PaymentService {

    private final PaymentRepository paymentRepository;
    private final PaymentMapper paymentMapper;
    private final PaymentClients clients;

    @Override
    public BigDecimal calculateProductsCost(UUID orderId) {
        log.info("Calculating products cost for order: {}", orderId);

        OrderDto order = clients.getOrderClient().getOrder(orderId);

        BigDecimal total = BigDecimal.ZERO;
        for (Map.Entry<UUID, Long> entry : order.getProducts().entrySet()) {
            ProductDto product = clients.getStoreClient().getProduct(entry.getKey());
            BigDecimal price = BigDecimal.valueOf(product.getPrice());
            BigDecimal quantity = BigDecimal.valueOf(entry.getValue());
            total = total.add(price.multiply(quantity));
        }

        log.info("Products total cost: {}", total);
        return total;
    }

    @Override
    public BigDecimal calculateTotalCost(UUID orderId) {
        log.info("Calculating total cost for order: {}", orderId);

        // 1. Стоимость товаров
        BigDecimal productsCost = calculateProductsCost(orderId);

        // 2. НДС 10%
        BigDecimal tax = productsCost.multiply(new BigDecimal("0.1"));

        // 3. Стоимость доставки
        OrderDto order = clients.getOrderClient().getOrder(orderId);
        BigDecimal deliveryCost = BigDecimal.ZERO;

        // Проверяем тип deliveryPrice и конвертируем правильно
        Object deliveryPriceObj = order.getDeliveryPrice();
        if (deliveryPriceObj instanceof Number) {
            deliveryCost = BigDecimal.valueOf(((Number) deliveryPriceObj).doubleValue());
        }

        // 4. Итог
        BigDecimal total = productsCost.add(tax).add(deliveryCost);

        log.info("Total cost: products={}, tax={}, delivery={}, total={}",
                productsCost, tax, deliveryCost, total);

        return total;
    }

    @Override
    @Transactional
    public PaymentDto createPayment(PaymentRequest request) {
        log.info("Creating payment for order: {}", request.getOrderId());

        BigDecimal productsCost = calculateProductsCost(request.getOrderId());
        BigDecimal deliveryCost = BigDecimal.ZERO;
        BigDecimal total = calculateTotalCost(request.getOrderId());

        Payment payment = Payment.builder()
                .orderId(request.getOrderId())
                .productsTotal(productsCost)
                .deliveryTotal(deliveryCost)
                .total(total)
                .status(PaymentStatus.PENDING)
                .build();

        payment = paymentRepository.save(payment);
        log.info("Payment created with id: {}", payment.getPaymentId());

        return paymentMapper.toDto(payment);
    }

    @Override
    @Transactional
    public void paymentSuccess(UUID paymentId) {
        log.info("Processing successful payment: {}", paymentId);

        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new RuntimeException("Payment not found: " + paymentId));

        payment.setStatus(PaymentStatus.SUCCESS);
        paymentRepository.save(payment);

        clients.getOrderClient().paymentSuccess(payment.getOrderId());

        log.info("Payment {} marked as SUCCESS", paymentId);
    }

    @Override
    @Transactional
    public void paymentFailed(UUID paymentId) {
        log.info("Processing failed payment: {}", paymentId);

        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new RuntimeException("Payment not found: " + paymentId));

        payment.setStatus(PaymentStatus.FAILED);
        paymentRepository.save(payment);

        clients.getOrderClient().paymentFailed(payment.getOrderId());

        log.info("Payment {} marked as FAILED", paymentId);
    }
}
