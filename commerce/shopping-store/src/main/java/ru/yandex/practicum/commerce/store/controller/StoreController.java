package ru.yandex.practicum.commerce.store.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.ProductDto;
import ru.yandex.practicum.commerce.dto.enums.ProductCategory;
import ru.yandex.practicum.commerce.dto.enums.ProductStatus;
import ru.yandex.practicum.commerce.store.service.StoreService;

import java.util.List;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/shopping-store")
@RequiredArgsConstructor
public class StoreController {

    private final StoreService storeService;

    @GetMapping("/products")
    public List<ProductDto> getProducts(
            @RequestParam(required = false) ProductCategory category) {
        log.info("GET /products with category: {}", category);
        if (category != null) {
            return storeService.getProductsByCategory(category);
        }
        return storeService.getAllActiveProducts();
    }

    @GetMapping("/products/{productId}")
    public ProductDto getProduct(@PathVariable UUID productId) {
        log.info("GET /products/{}", productId);
        return storeService.getProductById(productId);
    }

    @PostMapping("/products")
    @ResponseStatus(HttpStatus.CREATED)
    public ProductDto addProduct(@Valid @RequestBody ProductDto productDto) {
        log.info("POST /products - adding product: {}", productDto.getProductName());
        return storeService.createProduct(productDto);
    }

    @PutMapping("/products/{productId}")
    public ProductDto updateProduct(
            @PathVariable UUID productId,
            @Valid @RequestBody ProductDto productDto) {
        log.info("PUT /products/{}", productId);
        return storeService.updateProduct(productId, productDto);
    }

    @PostMapping("/products")
    @ResponseStatus(HttpStatus.CREATED)
    public ProductDto createProduct(@Valid @RequestBody ProductDto productDto) {
        log.info("POST /products - creating product: {}", productDto.getProductName());

        productDto.setStatus(ProductStatus.ACTIVE);

        return storeService.createProduct(productDto);
    }

    @DeleteMapping("/products/{productId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteProduct(@PathVariable UUID productId) {
        log.info("DELETE /products/{}", productId);
        storeService.deactivateProduct(productId);
    }

    @PatchMapping("/products/{productId}/quantity")
    @ResponseStatus(HttpStatus.OK)
    public ProductDto updateQuantity(
            @PathVariable UUID productId,
            @RequestParam Integer quantity) {
        log.info("PATCH /products/{}/quantity - quantity: {}", productId, quantity);
        storeService.updateProductQuantity(productId, quantity);
        return storeService.getProductById(productId);
    }
}
