package ru.yandex.practicum.commerce.cart.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.commerce.cart.NotAuthorizedUserException;
import ru.yandex.practicum.commerce.cart.service.CartService;
import ru.yandex.practicum.commerce.client.ShoppingCartClient;
import ru.yandex.practicum.commerce.dto.ShoppingCartDto;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/shopping-cart")
@RequiredArgsConstructor
public class CartController implements ShoppingCartClient {

    private final CartService cartService;

    @Override
    @GetMapping
    public ShoppingCartDto getShoppingCart(@RequestParam String username) {
        log.info("GET ?username={}", username);
        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Username is required");
        }
        return cartService.getCart(username);
    }

    @Override
    @DeleteMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deactivateShoppingCart(@RequestParam String username) {
        log.info("DELETE ?username={} - deactivating cart", username);
        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Username is required");
        }
        cartService.deactivateCart(username);
    }

    @PutMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ShoppingCartDto addProductToShoppingCart(
            @RequestParam String username,
            @RequestBody Map<UUID, Long> products) {
        log.info("PUT ?username={} - products: {}", username, products);
        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Username is required");
        }
        return cartService.addProductsToCart(username, products);
    }

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

    @PostMapping(value = "/change-quantity", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ShoppingCartDto changeProductQuantityWithBody(
            @RequestParam String username,
            @Valid @RequestBody ChangeProductQuantityRequest request) {
        log.info("POST /change-quantity?username={} (body) - product: {}, quantity: {}",
                username, request.getProductId(), request.getNewQuantity());
        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Username is required");
        }
        return cartService.changeProductQuantity(username, request);
    }

    @PostMapping(value = "/change-quantity", params = {"productId", "newQuantity"})
    public ShoppingCartDto changeProductQuantityWithParams(
            @RequestParam String username,
            @RequestParam UUID productId,
            @RequestParam Long newQuantity) {
        log.info("POST /change-quantity?username={} (params) - product: {}, quantity: {}",
                username, productId, newQuantity);
        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Username is required");
        }

        ChangeProductQuantityRequest request = ChangeProductQuantityRequest.builder()
                .productId(productId)
                .newQuantity(newQuantity)
                .build();

        return cartService.changeProductQuantity(username, request);
    }
}