package ru.yandex.practicum.commerce.cart.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.cart.service.CartService;
import ru.yandex.practicum.commerce.dto.AddProductToCartRequest;
import ru.yandex.practicum.commerce.dto.ShoppingCartDto;

import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/shopping-cart")
@RequiredArgsConstructor
public class CartController {

    private final CartService cartService;

    @GetMapping("/{username}")
    public ShoppingCartDto getShoppingCart(@PathVariable String username) {
        log.info("GET /shopping-cart/{}", username);
        return cartService.getCart(username);
    }

    @PostMapping("/{username}/add")
    @ResponseStatus(HttpStatus.CREATED)
    public ShoppingCartDto addProductToCart(
            @PathVariable String username,
            @Valid @RequestBody AddProductToCartRequest request) {
        log.info("POST /shopping-cart/{}/add - product: {}", username, request.getProductId());
        return cartService.addProductToCart(username, request);
    }

    @DeleteMapping("/{username}/remove/{productId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeProductFromCart(
            @PathVariable String username,
            @PathVariable UUID productId) {
        log.info("DELETE /shopping-cart/{}/remove/{}", username, productId);
        cartService.removeProductFromCart(username, productId);
    }

    @PatchMapping("/{username}/update/{productId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateProductQuantity(
            @PathVariable String username,
            @PathVariable UUID productId,
            @RequestParam Integer quantity) {
        log.info("PATCH /shopping-cart/{}/update/{} - quantity: {}", username, productId, quantity);
        cartService.updateProductQuantity(username, productId, quantity);
    }

    @DeleteMapping("/{username}/clear")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void clearCart(@PathVariable String username) {
        log.info("DELETE /shopping-cart/{}/clear", username);
        cartService.clearCart(username);
    }

    @DeleteMapping("/{username}/deactivate")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deactivateShoppingCart(@PathVariable String username) {
        log.info("DELETE /shopping-cart/{}/deactivate", username);
        cartService.deactivateCart(username);
    }

    @PutMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ShoppingCartDto addProductViaPut(
            @RequestParam String username,
            @Valid @RequestBody AddProductToCartRequest request) {
        log.info("PUT /shopping-cart?username={} - adding product via PUT", username);
        request.setUsername(username);
        return cartService.addProductToCart(username, request);
    }

    @PostMapping("/change-quantity")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void changeQuantityViaPost(
            @RequestParam String username,
            @RequestParam UUID productId,
            @RequestParam Integer quantity) {
        log.info("POST /shopping-cart/change-quantity?username={}&productId={}&quantity={}",
                username, productId, quantity);
        cartService.updateProductQuantity(username, productId, quantity);
    }

    @PostMapping("/remove")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeViaPost(
            @RequestParam String username,
            @RequestParam UUID productId) {
        log.info("POST /shopping-cart/remove?username={}&productId={}", username, productId);
        cartService.removeProductFromCart(username, productId);
    }

    @DeleteMapping
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deactivateViaDelete(@RequestParam String username) {
        log.info("DELETE /shopping-cart?username={} - deactivating cart", username);
        cartService.deactivateCart(username);
    }
}
