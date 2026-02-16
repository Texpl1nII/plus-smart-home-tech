package ru.yandex.practicum.commerce.warehouse.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.AddressDto;
import ru.yandex.practicum.commerce.dto.ProductAvailabilityRequest;
import ru.yandex.practicum.commerce.dto.ProductAvailabilityResponse;
import ru.yandex.practicum.commerce.dto.WarehouseProductDto;
import ru.yandex.practicum.commerce.warehouse.exception.ProductNotFoundException;
import ru.yandex.practicum.commerce.warehouse.mapper.WarehouseMapper;
import ru.yandex.practicum.commerce.warehouse.model.WarehouseProduct;
import ru.yandex.practicum.commerce.warehouse.repository.WarehouseProductRepository;
import ru.yandex.practicum.commerce.warehouse.service.WarehouseService;
import ru.yandex.practicum.commerce.warehouse.util.AddressGenerator;

import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class WarehouseServiceImpl implements WarehouseService {

    private final WarehouseProductRepository repository;
    private final WarehouseMapper mapper;
    private final AddressGenerator addressGenerator;

    @Override
    @Transactional
    public void addProductToWarehouse(WarehouseProductDto productDto) {
        log.info("Adding product to warehouse: {}", productDto.getProductId());

        WarehouseProduct product = mapper.toEntity(productDto);
        repository.save(product);

        log.info("Product added to warehouse successfully");
    }

    @Override
    @Transactional
    public void addQuantity(UUID productId, Integer quantity) {
        log.info("Adding quantity {} to product: {}", quantity, productId);

        WarehouseProduct product = repository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException(
                        "Product not found in warehouse: " + productId));

        mapper.updateQuantity(product, quantity);
        repository.save(product);

        log.info("Quantity updated successfully");
    }

    @Override
    public WarehouseProductDto getWarehouseProduct(UUID productId) {
        log.info("Getting warehouse product: {}", productId);

        WarehouseProduct product = repository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException(
                        "Product not found in warehouse: " + productId));

        return mapper.toDto(product);
    }

    @Override
    public ProductAvailabilityResponse checkAvailability(ProductAvailabilityRequest request) {
        log.info("Checking availability for {} products", request.getProducts().size());

        Set<UUID> productIds = request.getProducts().keySet();
        List<WarehouseProduct> availableProducts = repository.findByProductIdIn(productIds);

        Map<UUID, Integer> availableMap = new HashMap<>();
        for (WarehouseProduct wp : availableProducts) {
            availableMap.put(wp.getProductId(), wp.getQuantity());
        }

        List<UUID> unavailableProducts = new ArrayList<>();

        // Проверяем каждый товар из запроса
        for (Map.Entry<UUID, Integer> entry : request.getProducts().entrySet()) {
            UUID productId = entry.getKey();
            Integer requestedQuantity = entry.getValue();

            Integer availableQuantity = availableMap.getOrDefault(productId, 0);

            if (availableQuantity < requestedQuantity) {
                unavailableProducts.add(productId);
                log.debug("Product {} unavailable: requested {}, available {}",
                        productId, requestedQuantity, availableQuantity);
            }
        }

        boolean allAvailable = unavailableProducts.isEmpty();

        ProductAvailabilityResponse response = ProductAvailabilityResponse.builder()
                .available(allAvailable)
                .unavailableProducts(unavailableProducts)
                .build();

        log.info("Availability check result: available={}, unavailable={}",
                allAvailable, unavailableProducts.size());

        return response;
    }

    @Override
    public AddressDto getWarehouseAddress() {
        log.info("Getting warehouse address");
        return addressGenerator.getCurrentAddress();
    }
}
