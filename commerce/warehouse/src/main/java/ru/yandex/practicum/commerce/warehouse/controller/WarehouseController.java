package ru.yandex.practicum.commerce.warehouse.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.AddressDto;
import ru.yandex.practicum.commerce.dto.ProductAvailabilityRequest;
import ru.yandex.practicum.commerce.dto.ProductAvailabilityResponse;
import ru.yandex.practicum.commerce.dto.WarehouseProductDto;
import ru.yandex.practicum.commerce.warehouse.service.WarehouseService;

import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/warehouse")
@RequiredArgsConstructor
public class WarehouseController {

    private final WarehouseService warehouseService;

    // Эндпоинты для администрации
    @PostMapping("/products")
    @ResponseStatus(HttpStatus.CREATED)
    public void addProductToWarehouse(@Valid @RequestBody WarehouseProductDto productDto) {
        log.info("POST /warehouse/products - adding product: {}", productDto.getProductId());
        warehouseService.addProductToWarehouse(productDto);
    }

    @PatchMapping("/products/{productId}/quantity")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void addQuantity(
            @PathVariable UUID productId,
            @RequestParam Integer quantity) {
        log.info("PATCH /warehouse/products/{}/quantity - adding quantity: {}", productId, quantity);
        warehouseService.addQuantity(productId, quantity);
    }

    @GetMapping("/products/{productId}")
    public WarehouseProductDto getWarehouseProduct(@PathVariable UUID productId) {
        log.info("GET /warehouse/products/{}", productId);
        return warehouseService.getWarehouseProduct(productId);
    }

    @PostMapping("/check-availability")
    public ProductAvailabilityResponse checkAvailability(
            @Valid @RequestBody ProductAvailabilityRequest request) {
        log.info("POST /warehouse/check-availability for user: {}", request.getUsername());
        return warehouseService.checkAvailability(request);
    }

    @GetMapping("/address")
    public AddressDto getWarehouseAddress() {
        log.info("GET /warehouse/address");
        return warehouseService.getWarehouseAddress();
    }
}
