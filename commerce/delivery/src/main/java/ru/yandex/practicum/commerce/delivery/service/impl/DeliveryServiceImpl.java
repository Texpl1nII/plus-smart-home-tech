package ru.yandex.practicum.commerce.delivery.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.delivery.client.DeliveryClients;
import ru.yandex.practicum.commerce.delivery.mapper.DeliveryMapper;
import ru.yandex.practicum.commerce.delivery.model.Delivery;
import ru.yandex.practicum.commerce.delivery.repository.DeliveryRepository;
import ru.yandex.practicum.commerce.delivery.service.DeliveryService;
import ru.yandex.practicum.commerce.dto.delivery.DeliveryDto;
import ru.yandex.practicum.commerce.dto.delivery.DeliveryState;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeliveryServiceImpl implements DeliveryService {

    private final DeliveryRepository deliveryRepository;
    private final DeliveryMapper deliveryMapper;
    private final DeliveryClients clients;

    @Override
    @Transactional
    public DeliveryDto planDelivery(DeliveryDto deliveryDto) {
        log.info("Planning delivery for order: {}", deliveryDto.getOrderId());

        // Рассчитываем стоимость доставки
        double cost = calculateDeliveryCost(deliveryDto);

        Delivery delivery = deliveryMapper.toEntity(deliveryDto);
        delivery.setDeliveryCost(cost);
        delivery.setState(DeliveryState.CREATED);

        delivery = deliveryRepository.save(delivery);
        log.info("Delivery planned with id: {}, cost: {}", delivery.getDeliveryId(), cost);

        return deliveryMapper.toDto(delivery);
    }

    @Override
    public Double calculateDeliveryCost(DeliveryDto deliveryDto) {
        log.info("Calculating delivery cost for order: {}", deliveryDto.getOrderId());

        // Алгоритм из ТЗ
        double baseRate = 5.0;
        double cost = baseRate;

        // 1. Адрес склада (fromAddress)
        String warehouseAddress = deliveryDto.getFromAddress().getStreet();
        if (warehouseAddress.contains("ADDRESS_2")) {
            cost += baseRate * 2;  // умножаем на 2 и прибавляем к базовой
        } else if (warehouseAddress.contains("ADDRESS_1")) {
            cost += baseRate * 1;
        }

        // 2. Хрупкость
        if (Boolean.TRUE.equals(deliveryDto.getFragile())) {
            cost += cost * 0.2;
        }

        // 3. Вес
        cost += deliveryDto.getWeight() * 0.3;

        // 4. Объём
        cost += deliveryDto.getVolume() * 0.2;

        // 5. Адрес доставки (совпадение улицы)
        if (!deliveryDto.getFromAddress().getStreet()
                .equals(deliveryDto.getToAddress().getStreet())) {
            cost += cost * 0.2;
        }

        log.info("Calculated delivery cost: {}", cost);
        return cost;
    }

    @Override
    @Transactional
    public void deliverySuccess(UUID deliveryId) {
        log.info("Processing successful delivery: {}", deliveryId);

        Delivery delivery = deliveryRepository.findById(deliveryId)
                .orElseThrow(() -> new RuntimeException("Delivery not found: " + deliveryId));

        delivery.setState(DeliveryState.DELIVERED);
        deliveryRepository.save(delivery);

        // Уведомляем сервис заказов
        clients.getOrderClient().deliverySuccess(delivery.getOrderId());

        log.info("Delivery {} marked as DELIVERED", deliveryId);
    }

    @Override
    @Transactional
    public void deliveryFailed(UUID deliveryId) {
        log.info("Processing failed delivery: {}", deliveryId);

        Delivery delivery = deliveryRepository.findById(deliveryId)
                .orElseThrow(() -> new RuntimeException("Delivery not found: " + deliveryId));

        delivery.setState(DeliveryState.FAILED);
        deliveryRepository.save(delivery);

        // Уведомляем сервис заказов
        clients.getOrderClient().deliveryFailed(delivery.getOrderId());

        log.info("Delivery {} marked as FAILED", deliveryId);
    }
}
