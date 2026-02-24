package ru.yandex.practicum.commerce.cart.service;

import ru.yandex.practicum.commerce.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.commerce.dto.AddProductToCartRequest;
import ru.yandex.practicum.commerce.dto.ShoppingCartDto;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface CartService {

    // Существующие методы (ОБЯЗАТЕЛЬНЫ К РЕАЛИЗАЦИИ)
    ShoppingCartDto getCart(String username);

    ShoppingCartDto getOrCreateCart(String username);

    ShoppingCartDto addProductToCart(String username, AddProductToCartRequest request);

    void removeProductFromCart(String username, UUID productId);  // ← этот метод должен быть!

    void updateProductQuantity(String username, UUID productId, Integer newQuantity);

    void clearCart(String username);

    void deactivateCart(String username);

    // НОВЫЕ методы для соответствия спецификации (тоже обязательны)
    ShoppingCartDto addProductsToCart(String username, Map<UUID, Long> products);

    ShoppingCartDto removeProductsFromCart(String username, List<UUID> productIds);

    ShoppingCartDto changeProductQuantity(String username, ChangeProductQuantityRequest request);
}