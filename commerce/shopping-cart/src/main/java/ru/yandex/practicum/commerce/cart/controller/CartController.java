package ru.yandex.practicum.commerce.cart.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.commerce.cart.NotAuthorizedUserException;
import ru.yandex.practicum.commerce.dto.ShoppingCartDto;

import ru.yandex.practicum.commerce.cart.service.CartService;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/shopping-cart")
@RequiredArgsConstructor
public class CartController {

    private final CartService cartService;

    /**
     * GET /api/v1/shopping-cart?username=... - получение корзины пользователя
     */
    @GetMapping
    public ShoppingCartDto getShoppingCart(@RequestParam String username) {
        log.info("GET ?username={}", username);

        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Username is required");
        }

        return cartService.getCart(username);
    }

    /**
     * PUT /api/v1/shopping-cart?username=... - добавление товаров в корзину
     */
    @PutMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ShoppingCartDto addProductToShoppingCart(
            @RequestParam String username,
            @RequestBody Map<UUID, Long> products) {  // long в спецификации

        log.info("PUT ?username={} - products: {}", username, products);

        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Username is required");
        }

        return cartService.addProductsToCart(username, products);
    }

    /**
     * DELETE /api/v1/shopping-cart?username=... - деактивация корзины
     */
    @DeleteMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deactivateCurrentShoppingCart(@RequestParam String username) {
        log.info("DELETE ?username={} - deactivating cart", username);

        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Username is required");
        }

        cartService.deactivateCart(username);
    }

    /**
     * POST /api/v1/shopping-cart/remove?username=... - удаление товаров из корзины
     */
    @PostMapping("/remove")
    public ShoppingCartDto removeFromShoppingCart(
            @RequestParam String username,
            @RequestBody List<UUID> productIds) {

        log.info("POST /remove?username={} - removing products: {}", username, productIds);

        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Username is required");
        }

        if (productIds == null || productIds.isEmpty()) {
            throw new IllegalArgumentException("Product IDs list must not be empty");
        }

        return cartService.removeProductsFromCart(username, productIds);
    }

    /**
     * POST /api/v1/shopping-cart/change-quantity?username=... - изменение количества товара
     */
    @PostMapping("/change-quantity")
    public ShoppingCartDto changeProductQuantity(
            @RequestParam String username,
            @Valid @RequestBody ChangeProductQuantityRequest request) {

        log.info("POST /change-quantity?username={} - product: {}, quantity: {}",
                username, request.getProductId(), request.getNewQuantity());

        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Username is required");
        }

        return cartService.changeProductQuantity(username, request);
    }
}