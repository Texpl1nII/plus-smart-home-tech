package ru.yandex.practicum.commerce.store.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.ProductDto;
import ru.yandex.practicum.commerce.dto.enums.ProductCategory;
import ru.yandex.practicum.commerce.dto.enums.ProductStatus;
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
    public List<ProductDto> getProductsByCategory(ProductCategory category) {
        log.info("Getting products by category: {}", category);

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

        Product product = findActiveProductById(productId);
        return productMapper.toDto(product);
    }

    @Override
    @Transactional
    public ProductDto createProduct(ProductDto productDto) {
        log.info("Creating new product: {}", productDto.getProductName());

        if (productDto.getStatus() != ProductStatus.ACTIVE) {
            log.warn("Forcing status to ACTIVE for new product");
            productDto.setStatus(ProductStatus.ACTIVE);
        }

        Product product = productMapper.toEntity(productDto);

        if (product.getQuantity() == null) {
            product.setQuantity(0);
        }

        Product savedProduct = productRepository.save(product);
        log.info("Product created with id: {}", savedProduct.getId());

        return productMapper.toDto(savedProduct);
    }

    @Override
    @Transactional
    public ProductDto updateProduct(UUID productId, ProductDto productDto) {
        log.info("Updating product with id: {}", productId);

        Product product = findActiveProductById(productId);
        productMapper.updateProductFromDto(productDto, product);

        Product updatedProduct = productRepository.save(product);
        log.info("Product updated successfully");

        return productMapper.toDto(updatedProduct);
    }

    @Override
    @Transactional
    public void deactivateProduct(UUID productId) {
        log.info("Deactivating product with id: {}", productId);

        Product product = findActiveProductById(productId);
        product.setStatus(ProductStatus.DEACTIVATE);
        productRepository.save(product);

        log.info("Product deactivated successfully");
    }

    @Override
    @Transactional
    public void updateProductQuantity(UUID productId, Integer newQuantity) {
        log.info("Updating quantity for product {} to {}", productId, newQuantity);

        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException(
                        "Product not found with id: " + productId));

        product.setQuantity(newQuantity);
        productRepository.save(product);

        log.info("Product quantity updated successfully");
    }

    private Product findActiveProductById(UUID productId) {
        return productRepository.findById(productId)
                .filter(product -> product.getStatus() == ProductStatus.ACTIVE)
                .orElseThrow(() -> new ProductNotFoundException(
                        "Product not found with id: " + productId));
    }
}
