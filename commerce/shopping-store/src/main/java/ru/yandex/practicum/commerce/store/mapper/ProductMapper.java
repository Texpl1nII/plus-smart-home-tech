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
                .productCategory(product.getCategory())
                .price(product.getPrice())
                .productState(product.getStatus())
                .quantityState(product.getAvailability())
                .imageSrc(product.getImageUrl())
                .build();
    }

    public Product toEntity(ProductDto dto) {
        if (dto == null) {
            return null;
        }

        Product product = Product.builder()
                .id(dto.getProductId() != null ? dto.getProductId() : null)
                .name(dto.getProductName())
                .description(dto.getDescription())
                .category(dto.getProductCategory())
                .price(dto.getPrice())
                .status(dto.getProductState() != null ? dto.getProductState() : ProductStatus.ACTIVE)
                .imageUrl(dto.getImageSrc())
                .quantity(0)
                .availability(dto.getQuantityState() != null ? dto.getQuantityState() : AvailabilityStatus.ENDED)
                .build();

        // Если есть quantityState, устанавливаем соответствующее количество
        if (dto.getQuantityState() != null) {
            product.setQuantity(convertStateToQuantity(dto.getQuantityState()));
        }

        return product;
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
        if (dto.getProductCategory() != null) {
            product.setCategory(dto.getProductCategory());
        }
        if (dto.getPrice() != null) {
            product.setPrice(dto.getPrice());
        }
        if (dto.getProductState() != null) {
            product.setStatus(dto.getProductState());
        }
        if (dto.getQuantityState() != null) {
            product.setAvailability(dto.getQuantityState());
            product.setQuantity(convertStateToQuantity(dto.getQuantityState()));
        }
        if (dto.getImageSrc() != null) {
            product.setImageUrl(dto.getImageSrc());
        }
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
}