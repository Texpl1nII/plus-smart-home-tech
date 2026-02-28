package ru.yandex.practicum.commerce.delivery.mapper;

import org.mapstruct.Mapper;
import ru.yandex.practicum.commerce.delivery.model.Delivery;
import ru.yandex.practicum.commerce.dto.delivery.DeliveryDto;

@Mapper(componentModel = "spring")
public interface DeliveryMapper {

    DeliveryDto toDto(Delivery delivery);

    Delivery toEntity(DeliveryDto deliveryDto);
}