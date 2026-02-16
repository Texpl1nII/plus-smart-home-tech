package ru.yandex.practicum.commerce.store.service;

import ru.yandex.practicum.commerce.dto.ProductDto;
import ru.yandex.practicum.commerce.dto.enums.ProductCategory;
import ru.yandex.practicum.commerce.dto.enums.ProductStatus;

import java.util.List;
import java.util.UUID;

public interface StoreService {

    List<ProductDto> getProductsByCategory(ProductCategory category);

    List<ProductDto> getAllActiveProducts();

    ProductDto getProductById(UUID productId);

    ProductDto createProduct(ProductDto productDto);

    ProductDto updateProduct(UUID productId, ProductDto productDto);

    void deactivateProduct(UUID productId); // soft delete

    void updateProductQuantity(UUID productId, Integer newQuantity);
}