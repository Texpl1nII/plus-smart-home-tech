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
    public Double calculateProductsCost(UUID orderId) {
        log.info("Calculating products cost for order: {}", orderId);

        // Получаем заказ
        OrderDto order = clients.getOrderClient().getOrder(orderId);

        // Получаем цены товаров из shopping-store
        double total = 0.0;
        for (Map.Entry<UUID, Long> entry : order.getProducts().entrySet()) {
            ProductDto product = clients.getStoreClient().getProduct(entry.getKey());
            total += product.getPrice() * entry.getValue();
        }

        log.info("Products total cost: {}", total);
        return total;
    }

    @Override
    public Double calculateTotalCost(UUID orderId) {
        log.info("Calculating total cost for order: {}", orderId);

        // 1. Стоимость товаров
        double productsCost = calculateProductsCost(orderId);

        // 2. НДС 10%
        double tax = productsCost * 0.1;

        // 3. Получаем заказ (там уже есть стоимость доставки)
        OrderDto order = clients.getOrderClient().getOrder(orderId);
        double deliveryCost = order.getDeliveryPrice() != null ? order.getDeliveryPrice() : 0.0;

        // 4. Итог: товары + налог + доставка
        double total = productsCost + tax + deliveryCost;

        log.info("Total cost: products={}, tax={}, delivery={}, total={}",
                productsCost, tax, deliveryCost, total);

        return total;
    }

    @Override
    @Transactional
    public PaymentDto createPayment(PaymentRequest request) {
        log.info("Creating payment for order: {}", request.getOrderId());

        // Рассчитываем стоимости
        double productsCost = calculateProductsCost(request.getOrderId());
        double deliveryCost = 0.0; // будет получено из заказа
        double total = calculateTotalCost(request.getOrderId());

        // Создаём платёж
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

        // Уведомляем сервис заказов
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

        // Уведомляем сервис заказов
        clients.getOrderClient().paymentFailed(payment.getOrderId());

        log.info("Payment {} marked as FAILED", paymentId);
    }
}
