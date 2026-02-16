package ru.yandex.practicum.commerce.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.ShoppingCartDto;

@FeignClient(name = "shopping-cart")
public interface ShoppingCartClient {

    @GetMapping("/api/v1/shopping-cart/{username}")
    ShoppingCartDto getShoppingCart(@PathVariable("username") String username);

    @DeleteMapping("/api/v1/shopping-cart/{username}/deactivate")
    void deactivateShoppingCart(@PathVariable("username") String username);
}
