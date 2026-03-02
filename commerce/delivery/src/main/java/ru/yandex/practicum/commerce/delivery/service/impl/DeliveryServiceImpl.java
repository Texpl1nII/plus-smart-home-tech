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
import ru.yandex.practicum.commerce.dto.enums.DeliveryState;

import java.math.BigDecimal;
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

        BigDecimal cost = calculateDeliveryCost(deliveryDto);  // теперь OK

        Delivery delivery = deliveryMapper.toEntity(deliveryDto);
        delivery.setDeliveryCost(cost);
        delivery.setState(DeliveryState.CREATED);

        delivery = deliveryRepository.save(delivery);
        log.info("Delivery planned with id: {}, cost: {}", delivery.getDeliveryId(), cost);

        return deliveryMapper.toDto(delivery);
    }

    @Override
    public BigDecimal calculateDeliveryCost(DeliveryDto deliveryDto) {  // long -> BigDecimal
        log.info("Calculating delivery cost for order: {}", deliveryDto.getOrderId());

        BigDecimal baseRate = new BigDecimal("5.0");
        BigDecimal cost = baseRate;

        // 1. Адрес склада
        String warehouseAddress = deliveryDto.getFromAddress().getStreet();
        if (warehouseAddress != null && warehouseAddress.contains("ADDRESS_2")) {
            cost = cost.add(baseRate.multiply(new BigDecimal("2")));
        } else if (warehouseAddress != null && warehouseAddress.contains("ADDRESS_1")) {
            cost = cost.add(baseRate.multiply(new BigDecimal("1")));
        }

        // 2. Хрупкость
        if (Boolean.TRUE.equals(deliveryDto.getFragile())) {
            cost = cost.add(cost.multiply(new BigDecimal("0.2")));
        }

        // 3. Вес
        if (deliveryDto.getWeight() != null) {
            cost = cost.add(new BigDecimal(deliveryDto.getWeight().toString())
                    .multiply(new BigDecimal("0.3")));
        }

        // 4. Объём
        if (deliveryDto.getVolume() != null) {
            cost = cost.add(new BigDecimal(deliveryDto.getVolume().toString())
                    .multiply(new BigDecimal("0.2")));
        }

        // 5. Адрес доставки
        if (deliveryDto.getFromAddress() != null && deliveryDto.getToAddress() != null &&
                !deliveryDto.getFromAddress().getStreet()
                        .equals(deliveryDto.getToAddress().getStreet())) {
            cost = cost.add(cost.multiply(new BigDecimal("0.2")));
        }

        return cost;  // теперь возвращаем BigDecimal
    }

    @Override
    @Transactional
    public void deliverySuccess(UUID deliveryId) {
        log.info("Processing successful delivery: {}", deliveryId);

        Delivery delivery = deliveryRepository.findById(deliveryId)
                .orElseThrow(() -> new RuntimeException("Delivery not found: " + deliveryId));

        delivery.setState(DeliveryState.DELIVERED);
        deliveryRepository.save(delivery);

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

        clients.getOrderClient().deliveryFailed(delivery.getOrderId());

        log.info("Delivery {} marked as FAILED", deliveryId);
    }
}
