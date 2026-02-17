package ru.yandex.practicum.commerce.store.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.commerce.dto.ProductDto;
import ru.yandex.practicum.commerce.dto.enums.AvailabilityStatus;
import ru.yandex.practicum.commerce.dto.enums.ProductStatus;
import ru.yandex.practicum.commerce.store.model.Product;

import java.util.UUID;

@Component
public class ProductMapper {

    public ProductDto toDto(Product product) {
        if (product == null) return null;

        return ProductDto.builder()
                .productId(product.getId())
                .productName(product.getName())
                .description(product.getDescription())
                .category(product.getCategory())
                .price(product.getPrice())
                .status(product.getStatus())
                .availability(calculateAvailability(product.getQuantity()))
                .imageUrl(product.getImageUrl())
                .build();
    }

    public Product toEntity(ProductDto dto) {
        if (dto == null) {
            return null;
        }

        return Product.builder()
                .id(dto.getProductId() != null ? dto.getProductId() : UUID.randomUUID())
                .name(dto.getProductName())
                .description(dto.getDescription())
                .category(dto.getCategory())
                .price(dto.getPrice())
                .status(dto.getStatus() != null ? dto.getStatus() : ProductStatus.ACTIVE)
                .imageUrl(dto.getImageUrl())
                .quantity(0) // начальное количество
                .availability(dto.getAvailability() != null ? dto.getAvailability() : AvailabilityStatus.ENDED)
                .build();
    }

    public AvailabilityStatus calculateAvailability(Integer quantity) {
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

    public void updateProductFromDto(ProductDto dto, Product product) {
        if (dto == null || product == null) {
            return;
        }

        if (dto.getProductName() != null) {
            product.setName(dto.getProductName());
        }
        if (dto.getDescription() != null) {
            product.setDescription(dto.getDescription());
        }
        if (dto.getCategory() != null) {
            product.setCategory(dto.getCategory());
        }
        if (dto.getPrice() != null) {
            product.setPrice(dto.getPrice());
        }
        if (dto.getStatus() != null) {
            product.setStatus(dto.getStatus());
        }
        if (dto.getImageUrl() != null) {
            product.setImageUrl(dto.getImageUrl());
        }
    }
}