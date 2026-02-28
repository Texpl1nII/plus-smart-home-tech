package ru.yandex.practicum.commerce.delivery.service;

import ru.yandex.practicum.commerce.dto.delivery.DeliveryDto;
import java.util.UUID;

public interface DeliveryService {

    DeliveryDto planDelivery(DeliveryDto deliveryDto);

    Double calculateDeliveryCost(DeliveryDto deliveryDto);

    void deliverySuccess(UUID deliveryId);

    void deliveryFailed(UUID deliveryId);
}
