package ru.yandex.practicum.commerce.warehouse.service;

import ru.yandex.practicum.commerce.dto.*;
import ru.yandex.practicum.commerce.warehouse.AddProductToWarehouseRequest;
import ru.yandex.practicum.commerce.warehouse.BookedProductsDto;
import ru.yandex.practicum.commerce.warehouse.NewProductInWarehouseRequest;

import java.util.Map;
import java.util.UUID;

public interface WarehouseService {

    // Существующие методы
    void addProductToWarehouse(WarehouseProductDto productDto);

    void addQuantity(UUID productId, Integer quantity);

    WarehouseProductDto getWarehouseProduct(UUID productId);

    ProductAvailabilityResponse checkAvailability(ProductAvailabilityRequest request);

    AddressDto getWarehouseAddress();

    // НОВЫЕ МЕТОДЫ для соответствия спецификации
    boolean productExists(UUID productId);

    void addNewProduct(NewProductInWarehouseRequest request);

    BookedProductsDto checkAvailabilityForCart(ShoppingCartDto cart);

    void addProductQuantity(AddProductToWarehouseRequest request);

    Map<UUID, Integer> getUnavailableProducts(ShoppingCartDto cart);

    void assemblyProductForOrderFromShoppingCart(UUID orderId);

    void shippedToDelivery(UUID orderId, UUID deliveryId);

    void returnProduct(UUID orderId, Map<UUID, Long> products);
}
