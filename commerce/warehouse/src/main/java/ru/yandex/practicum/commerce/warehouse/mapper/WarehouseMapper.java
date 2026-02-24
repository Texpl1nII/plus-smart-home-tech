package ru.yandex.practicum.commerce.warehouse.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.commerce.dto.WarehouseProductDto;
import ru.yandex.practicum.commerce.warehouse.model.WarehouseProduct;

@Component
public class WarehouseMapper {

    public WarehouseProductDto toDto(WarehouseProduct product) {
        if (product == null) {
            return null;
        }

        return WarehouseProductDto.builder()
                .productId(product.getProductId())
                .quantity(product.getQuantity())
                .width(product.getWidth())
                .height(product.getHeight())
                .depth(product.getDepth())
                .weight(product.getWeight())
                .fragile(product.getFragile())
                .build();
    }

    public WarehouseProduct toEntity(WarehouseProductDto dto) {
        if (dto == null) {
            return null;
        }

        return WarehouseProduct.builder()
                .productId(dto.getProductId())
                .quantity(dto.getQuantity())
                .width(dto.getWidth())
                .height(dto.getHeight())
                .depth(dto.getDepth())
                .weight(dto.getWeight())
                .fragile(dto.getFragile())
                .build();
    }

    public void updateQuantity(WarehouseProduct product, Integer newQuantity) {
        if (product != null && newQuantity != null) {
            product.setQuantity(product.getQuantity() + newQuantity);
        }
    }
}
