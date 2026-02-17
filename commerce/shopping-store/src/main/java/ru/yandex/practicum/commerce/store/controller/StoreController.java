package ru.yandex.practicum.commerce.store.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.ProductDto;
import ru.yandex.practicum.commerce.dto.enums.ProductCategory;
import ru.yandex.practicum.commerce.store.SetProductQuantityStateRequest;
import ru.yandex.practicum.commerce.store.service.StoreService;

import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/shopping-store")
@RequiredArgsConstructor
public class StoreController {

    private final StoreService storeService;

    @GetMapping
    public Page<ProductDto> getProducts(
            @RequestParam ProductCategory category,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "productName,asc") String[] sort) {

        log.info("GET /?category={}&page={}&size={}&sort={}", category, page, size, (Object[]) sort);

        // Преобразуем параметры сортировки
        Sort sortBy = parseSort(sort);
        Pageable pageable = PageRequest.of(page, size, sortBy);

        return storeService.getProductsByCategory(category, pageable);
    }


    @PutMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ProductDto createNewProduct(@Valid @RequestBody ProductDto productDto) {
        log.info("PUT / - creating product: {}", productDto.getProductName());
        return storeService.createProduct(productDto);
    }

    @PostMapping
    public ProductDto updateProduct(@Valid @RequestBody ProductDto productDto) {
        log.info("POST / - updating product: {}", productDto.getProductId());

        if (productDto.getProductId() == null) {
            throw new IllegalArgumentException("Product ID must not be null for update");
        }

        return storeService.updateProduct(productDto.getProductId(), productDto);
    }

    @GetMapping("/{productId}")
    public ProductDto getProduct(@PathVariable UUID productId) {
        log.info("GET /{}", productId);
        return storeService.getProductById(productId);
    }

    @PostMapping("/removeProductFromStore")
    public boolean removeProductFromStore(@RequestBody UUID productId) {
        log.info("POST /removeProductFromStore - removing product: {}", productId);
        return storeService.deactivateProduct(productId);
    }

    @PostMapping("/quantityState")
    public boolean setProductQuantityState(@Valid @RequestBody SetProductQuantityStateRequest request) {
        log.info("POST /quantityState - product: {}, state: {}",
                request.getProductId(), request.getQuantityState());
        return storeService.setProductQuantityState(request);
    }

    private Sort parseSort(String[] sort) {
        if (sort == null || sort.length == 0) {
            return Sort.by("name").ascending();  // было "productName", стало "name"
        }

        Sort.Order[] orders = new Sort.Order[sort.length];
        for (int i = 0; i < sort.length; i++) {
            String[] parts = sort[i].split(",");
            String property = parts[0];

            // Маппинг полей из спецификации в имена полей модели
            if ("productName".equals(property)) {
                property = "name";
            } else if ("productCategory".equals(property)) {
                property = "category";
            } else if ("productState".equals(property)) {
                property = "status";
            } else if ("quantityState".equals(property)) {
                property = "availability";
            }

            Sort.Direction direction = parts.length > 1 && "desc".equalsIgnoreCase(parts[1])
                    ? Sort.Direction.DESC : Sort.Direction.ASC;
            orders[i] = new Sort.Order(direction, property).ignoreCase();
        }

        return Sort.by(orders);
    }
}
