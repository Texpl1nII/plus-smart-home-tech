package ru.yandex.practicum.commerce.warehouse.service;

import ru.yandex.practicum.commerce.dto.AddressDto;
import ru.yandex.practicum.commerce.dto.ProductAvailabilityRequest;
import ru.yandex.practicum.commerce.dto.ProductAvailabilityResponse;
import ru.yandex.practicum.commerce.dto.WarehouseProductDto;

import java.util.UUID;

public interface WarehouseService {

    // Для администрации
    void addProductToWarehouse(WarehouseProductDto productDto);

    void addQuantity(UUID productId, Integer quantity);

    WarehouseProductDto getWarehouseProduct(UUID productId);

    // Для других сервисов
    ProductAvailabilityResponse checkAvailability(ProductAvailabilityRequest request);

    AddressDto getWarehouseAddress();
}
