package ru.yandex.practicum.commerce.store.service;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import ru.yandex.practicum.commerce.dto.ProductDto;
import ru.yandex.practicum.commerce.dto.enums.ProductCategory;
import ru.yandex.practicum.commerce.store.SetProductQuantityStateRequest;

import java.util.List;
import java.util.UUID;

public interface StoreService {

    Page<ProductDto> getProductsByCategory(ProductCategory category, Pageable pageable);

    // Добавлен метод для получения списка без пагинации
    List<ProductDto> getProductsByCategoryOld(ProductCategory category);

    default List<ProductDto> getProductsByCategory(ProductCategory category) {
        Page<ProductDto> page = getProductsByCategory(category, Pageable.unpaged());
        return page.getContent();
    }

    List<ProductDto> getAllActiveProducts();

    ProductDto getProductById(UUID productId);

    ProductDto createProduct(ProductDto productDto);

    ProductDto updateProduct(UUID productId, ProductDto productDto);

    boolean deactivateProduct(UUID productId);

    void updateProductQuantity(UUID productId, Integer newQuantity);

    boolean setProductQuantityState(SetProductQuantityStateRequest request);
}