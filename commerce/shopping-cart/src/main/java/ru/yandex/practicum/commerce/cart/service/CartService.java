package ru.yandex.practicum.commerce.cart.service;

import ru.yandex.practicum.commerce.dto.ShoppingCartDto;
import ru.yandex.practicum.commerce.dto.AddProductToCartRequest;

import java.util.UUID;

public interface CartService {

    ShoppingCartDto getCart(String username);

    ShoppingCartDto addProductToCart(String username, AddProductToCartRequest request);

    void removeProductFromCart(String username, UUID productId);

    void updateProductQuantity(String username, UUID productId, Integer newQuantity);

    void clearCart(String username);

    void deactivateCart(String username);
}