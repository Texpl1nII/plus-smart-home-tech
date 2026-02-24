package ru.yandex.practicum.commerce.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.enums.ProductCategory;
import ru.yandex.practicum.commerce.dto.ProductDto;

import java.util.List;
import java.util.UUID;

@FeignClient(name = "shopping-store")
public interface StoreClient {

    @GetMapping("/api/v1/shopping-store/{productId}")
    ProductDto getProduct(@PathVariable("productId") UUID productId);

    @GetMapping("/api/v1/shopping-store")
    List<ProductDto> getProductsByCategory(@RequestParam("category") ProductCategory category);
}
