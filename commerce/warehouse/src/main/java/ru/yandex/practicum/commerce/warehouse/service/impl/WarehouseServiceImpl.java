package ru.yandex.practicum.commerce.warehouse.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.*;
import ru.yandex.practicum.commerce.warehouse.AddProductToWarehouseRequest;
import ru.yandex.practicum.commerce.warehouse.BookedProductsDto;
import ru.yandex.practicum.commerce.warehouse.NewProductInWarehouseRequest;
import ru.yandex.practicum.commerce.warehouse.exception.ProductNotFoundException;
import ru.yandex.practicum.commerce.warehouse.mapper.WarehouseMapper;
import ru.yandex.practicum.commerce.warehouse.model.WarehouseProduct;
import ru.yandex.practicum.commerce.warehouse.repository.WarehouseProductRepository;
import ru.yandex.practicum.commerce.warehouse.service.WarehouseService;
import ru.yandex.practicum.commerce.warehouse.util.AddressGenerator;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    public void addProductQuantity(AddProductToWarehouseRequest request) {
        log.info("Adding quantity {} to product: {}", request.getQuantity(), request.getProductId());

        WarehouseProduct product = repository.findById(request.getProductId())
                .orElseThrow(() -> new ProductNotFoundException(
                        "Product not found in warehouse: " + request.getProductId()));

        product.setQuantity(product.getQuantity() + request.getQuantity().intValue());
        repository.save(product);

        log.info("Quantity added successfully");
    }

    @Override
    public boolean productExists(UUID productId) {
        return repository.existsById(productId);
    }

    @Override
    @Transactional
    public void addNewProduct(NewProductInWarehouseRequest request) {
        log.info("Adding new product to warehouse: {}", request.getProductId());

        if (repository.existsById(request.getProductId())) {
            throw new ru.yandex.practicum.commerce.warehouse.exceptions.SpecifiedProductAlreadyInWarehouseException(request.getProductId());
        }

        WarehouseProduct product = WarehouseProduct.builder()
                .productId(request.getProductId())
                .quantity(0)
                .width(request.getDimension().getWidth())
                .height(request.getDimension().getHeight())
                .depth(request.getDimension().getDepth())
                .weight(request.getWeight())
                .fragile(request.getFragile())
                .build();

        repository.save(product);
        log.info("New product added to warehouse successfully");
    }

    @Override
    public Map<UUID, Integer> getUnavailableProducts(ShoppingCartDto cart) {
        Map<UUID, Integer> unavailable = new HashMap<>();

        if (cart.getProducts() == null || cart.getProducts().isEmpty()) {
            return unavailable;
        }

        Set<UUID> productIds = cart.getProducts().keySet();
        List<WarehouseProduct> availableProducts = repository.findByProductIdIn(productIds);

        Map<UUID, WarehouseProduct> productMap = availableProducts.stream()
                .collect(Collectors.toMap(WarehouseProduct::getProductId, Function.identity()));

        for (Map.Entry<UUID, Long> entry : cart.getProducts().entrySet()) {
            UUID productId = entry.getKey();
            Long requestedQuantity = entry.getValue();

            WarehouseProduct wp = productMap.get(productId);
            int available = wp != null ? wp.getQuantity() : 0;

            if (available < requestedQuantity.intValue()) {
                unavailable.put(productId, requestedQuantity.intValue() - available);
            }
        }

        return unavailable;
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
    public BookedProductsDto checkAvailabilityForCart(ShoppingCartDto cart) {
        log.info("Checking availability for cart: {}", cart.getShoppingCartId());

        if (cart.getProducts() == null || cart.getProducts().isEmpty()) {
            return BookedProductsDto.builder()
                    .deliveryWeight(0.0)
                    .deliveryVolume(0.0)
                    .fragile(false)
                    .build();
        }

        Set<UUID> productIds = cart.getProducts().keySet();
        List<WarehouseProduct> availableProducts = repository.findByProductIdIn(productIds);

        Map<UUID, WarehouseProduct> productMap = availableProducts.stream()
                .collect(Collectors.toMap(WarehouseProduct::getProductId, Function.identity()));

        double totalWeight = 0.0;
        double totalVolume = 0.0;
        boolean hasFragile = false;

        for (Map.Entry<UUID, Long> entry : cart.getProducts().entrySet()) {
            UUID productId = entry.getKey();
            Long requestedQuantity = entry.getValue();

            WarehouseProduct wp = productMap.get(productId);
            if (wp == null || wp.getQuantity() < requestedQuantity.intValue()) {
                log.debug("Product {} not available in requested quantity", productId);
                return null;
            }

            double volume = wp.getWidth() * wp.getHeight() * wp.getDepth();
            totalVolume += volume * requestedQuantity;
            totalWeight += wp.getWeight() * requestedQuantity;

            if (Boolean.TRUE.equals(wp.getFragile())) {
                hasFragile = true;
            }
        }

        return BookedProductsDto.builder()
                .deliveryWeight(totalWeight)
                .deliveryVolume(totalVolume)
                .fragile(hasFragile)
                .build();
    }

    @Override
    public AddressDto getWarehouseAddress() {
        log.info("Getting warehouse address");
        return addressGenerator.getCurrentAddress();
    }
}
