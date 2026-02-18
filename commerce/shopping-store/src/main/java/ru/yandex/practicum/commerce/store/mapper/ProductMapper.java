package ru.yandex.practicum.commerce.store.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.commerce.dto.ProductDto;
import ru.yandex.practicum.commerce.dto.enums.AvailabilityStatus;
import ru.yandex.practicum.commerce.dto.enums.ProductStatus;
import ru.yandex.practicum.commerce.store.model.Product;

@Component
public class ProductMapper {

    public ProductDto toDto(Product product) {
        if (product == null) {
            return null;
        }

        return ProductDto.builder()
                .productId(product.getId())
                .productName(product.getName())
                .description(product.getDescription())
                .productCategory(product.getCategory())  // ← Важно: productCategory!
                .price(product.getPrice())
                .productState(product.getStatus())       // ← Важно: productState!
                .quantityState(product.getAvailability()) // ← Важно: quantityState!
                .imageSrc(product.getImageUrl())         // ← Важно: imageSrc!
                .build();
    }

    public Product toEntity(ProductDto dto) {
        if (dto == null) {
            return null;
        }

        return Product.builder()
                .id(dto.getProductId())
                .name(dto.getProductName())
                .description(dto.getDescription())
                .category(dto.getProductCategory())      // ← Важно: берем из productCategory!
                .price(dto.getPrice())
                .status(dto.getProductState() != null ? dto.getProductState() : ProductStatus.ACTIVE)
                .availability(dto.getQuantityState() != null ? dto.getQuantityState() : AvailabilityStatus.ENDED)
                .imageUrl(dto.getImageSrc())
                .quantity(0) // начальное количество
                .build();
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
        if (dto.getProductCategory() != null) {        // ← Важно: productCategory!
            product.setCategory(dto.getProductCategory());
        }
        if (dto.getPrice() != null) {
            product.setPrice(dto.getPrice());
        }
        if (dto.getProductState() != null) {           // ← Важно: productState!
            product.setStatus(dto.getProductState());
        }
        if (dto.getQuantityState() != null) {          // ← Важно: quantityState!
            product.setAvailability(dto.getQuantityState());
        }
        if (dto.getImageSrc() != null) {               // ← Важно: imageSrc!
            product.setImageUrl(dto.getImageSrc());
        }
    }
}