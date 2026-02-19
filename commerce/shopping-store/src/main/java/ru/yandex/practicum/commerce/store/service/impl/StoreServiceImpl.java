package ru.yandex.practicum.commerce.store.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.ProductDto;
import ru.yandex.practicum.commerce.dto.enums.AvailabilityStatus;
import ru.yandex.practicum.commerce.dto.enums.ProductCategory;
import ru.yandex.practicum.commerce.dto.enums.ProductStatus;
import ru.yandex.practicum.commerce.store.SetProductQuantityStateRequest;
import ru.yandex.practicum.commerce.store.exception.ProductNotFoundException;
import ru.yandex.practicum.commerce.store.mapper.ProductMapper;
import ru.yandex.practicum.commerce.store.model.Product;
import ru.yandex.practicum.commerce.store.repository.ProductRepository;
import ru.yandex.practicum.commerce.store.service.StoreService;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class StoreServiceImpl implements StoreService {

    private final ProductRepository productRepository;
    private final ProductMapper productMapper;

    @Override
    public Page<ProductDto> getProductsByCategory(ProductCategory category, Pageable pageable) {
        log.info("Getting products by category: {} with pageable: {}", category, pageable);

        Page<Product> productPage = productRepository.findByCategoryAndStatus(
                category, ProductStatus.ACTIVE, pageable);

        log.info("Found {} products", productPage.getNumberOfElements());
        return productPage.map(productMapper::toDto);
    }

    @Override
    public List<ProductDto> getProductsByCategoryOld(ProductCategory category) {
        log.info("Getting products by category (old): {}", category);
        return productRepository.findByCategoryAndStatus(category, ProductStatus.ACTIVE)
                .stream()
                .map(productMapper::toDto)
                .collect(Collectors.toList());
    }

    @Override
    public List<ProductDto> getAllActiveProducts() {
        log.info("Getting all active products");
        return productRepository.findByStatus(ProductStatus.ACTIVE)
                .stream()
                .map(productMapper::toDto)
                .collect(Collectors.toList());
    }

    @Override
    public ProductDto getProductById(UUID productId) {
        log.info("Getting product by id: {}", productId);
        Product product = findAnyProductById(productId);
        return productMapper.toDto(product);
    }

    @Override
    @Transactional
    public ProductDto createProduct(ProductDto productDto) {
        log.info("Creating new product: {}", productDto.getProductName());

        Product product = productMapper.toEntity(productDto);

        if (product.getStatus() == null) {
            product.setStatus(ProductStatus.ACTIVE);
        }

        Product savedProduct = productRepository.save(product);
        log.info("Product created with id: {}", savedProduct.getId());

        productRepository.flush();

        return productMapper.toDto(savedProduct);
    }

    @Override
    @Transactional
    public ProductDto updateProduct(UUID productId, ProductDto productDto) {
        log.info("Updating product with id: {}", productId);

        Product product = findProductById(productId);  // ← ИЗМЕНЕНО!
        productMapper.updateProductFromDto(productDto, product);

        Product updatedProduct = productRepository.save(product);
        log.info("Product updated successfully");

        return productMapper.toDto(updatedProduct);
    }

    @Override
    @Transactional
    public boolean deactivateProduct(UUID productId) {
        log.info("Deactivating product with id: {}", productId);

        try {
            Product product = findAnyProductById(productId);
            product.setStatus(ProductStatus.DEACTIVATE);
            productRepository.save(product);
            log.info("Product deactivated successfully");
            return true;
        } catch (Exception e) {
            log.error("Error deactivating product", e);
            return false;
        }
    }

    @Override
    @Transactional
    public void updateProductQuantity(UUID productId, Integer newQuantity) {
        log.info("Updating quantity for product {}: {}", productId, newQuantity);

        Product product = findAnyProductById(productId);  // ← ИЗМЕНЕНО!
        product.setQuantity(newQuantity);
        product.setAvailability(calculateAvailability(newQuantity));
        productRepository.save(product);

        log.info("Product quantity updated");
    }

    @Override
    @Transactional
    public boolean setProductQuantityState(SetProductQuantityStateRequest request) {
        log.info("Setting quantity state for product: {} to {}",
                request.getProductId(), request.getQuantityState());

        try {
            Product product = findAnyProductById(request.getProductId());

            int quantity = convertStateToQuantity(request.getQuantityState());
            product.setQuantity(quantity);
            product.setAvailability(request.getQuantityState());

            productRepository.save(product);
            productRepository.flush();

            log.info("Product quantity state updated to: {}, quantity set to: {}",
                    request.getQuantityState(), quantity);
            return true;
        } catch (Exception e) {
            log.error("Error setting quantity state", e);
            return false;
        }
    }

    private Product findProductById(UUID productId) {
        return productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException(
                        "Product not found with id: " + productId));
    }

    private Product findActiveProductById(UUID productId) {
        return productRepository.findById(productId)
                .filter(product -> product.getStatus() == ProductStatus.ACTIVE)
                .orElseThrow(() -> new ProductNotFoundException(
                        "Product not found or not active with id: " + productId));
    }

    private Product findAnyProductById(UUID productId) {
        return productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException(
                        "Product not found with id: " + productId));
    }

    private int convertStateToQuantity(AvailabilityStatus state) {
        switch (state) {
            case ENDED: return 0;
            case FEW: return 5;
            case ENOUGH: return 50;
            case MANY: return 200;
            default: return 0;
        }
    }

    private AvailabilityStatus calculateAvailability(Integer quantity) {
        if (quantity == null || quantity <= 0) {
            return AvailabilityStatus.ENDED;
        } else if (quantity < 10) {
            return AvailabilityStatus.FEW;
        } else if (quantity <= 100) {
            return AvailabilityStatus.ENOUGH;
        } else {
            return AvailabilityStatus.MANY;
        }
    }
}