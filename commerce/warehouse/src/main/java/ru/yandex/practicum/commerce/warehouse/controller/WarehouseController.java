package ru.yandex.practicum.commerce.warehouse.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.AddressDto;
import ru.yandex.practicum.commerce.dto.ProductAvailabilityRequest;
import ru.yandex.practicum.commerce.dto.ProductAvailabilityResponse;
import ru.yandex.practicum.commerce.dto.ShoppingCartDto;
import ru.yandex.practicum.commerce.warehouse.AddProductToWarehouseRequest;
import ru.yandex.practicum.commerce.warehouse.BookedProductsDto;
import ru.yandex.practicum.commerce.warehouse.NewProductInWarehouseRequest;
import ru.yandex.practicum.commerce.warehouse.exceptions.NoSpecifiedProductInWarehouseException;
import ru.yandex.practicum.commerce.warehouse.exceptions.ProductInShoppingCartLowQuantityInWarehouse;
import ru.yandex.practicum.commerce.warehouse.exceptions.SpecifiedProductAlreadyInWarehouseException;
import ru.yandex.practicum.commerce.warehouse.service.WarehouseService;

@Slf4j
@RestController
@RequestMapping("/api/v1/warehouse")
@RequiredArgsConstructor
public class WarehouseController {

    private final WarehouseService warehouseService;

    @PutMapping
    @ResponseStatus(HttpStatus.CREATED)
    public void newProductInWarehouse(@Valid @RequestBody NewProductInWarehouseRequest request) {
        log.info("PUT / - adding new product: {}", request.getProductId());

        if (warehouseService.productExists(request.getProductId())) {
            throw new SpecifiedProductAlreadyInWarehouseException(request.getProductId());
        }

        warehouseService.addNewProduct(request);
    }

    @PostMapping("/check")
    public ProductAvailabilityResponse checkAvailability(
            @Valid @RequestBody ProductAvailabilityRequest request) {
        log.info("POST /check for user: {}", request.getUsername());
        return warehouseService.checkAvailability(request);
    }

    @PostMapping("/check")
    public BookedProductsDto checkProductQuantityEnoughForShoppingCart(
            @RequestBody ShoppingCartDto cart) {
        log.info("POST /check - checking cart: {}", cart.getShoppingCartId());

        BookedProductsDto result = warehouseService.checkAvailabilityForCart(cart);

        if (result == null) {
            throw new ProductInShoppingCartLowQuantityInWarehouse(
                    warehouseService.getUnavailableProducts(cart)
            );
        }

        return result;
    }

    @PostMapping("/add")
    @ResponseStatus(HttpStatus.CREATED)
    public void addProductToWarehouse(@Valid @RequestBody AddProductToWarehouseRequest request) {
        log.info("POST /add - adding quantity to product: {}", request.getProductId());

        if (!warehouseService.productExists(request.getProductId())) {
            throw new NoSpecifiedProductInWarehouseException(request.getProductId());
        }

        warehouseService.addProductQuantity(request);
    }

    @GetMapping("/address")
    public AddressDto getWarehouseAddress() {
        log.info("GET /address");
        return warehouseService.getWarehouseAddress();
    }
}
