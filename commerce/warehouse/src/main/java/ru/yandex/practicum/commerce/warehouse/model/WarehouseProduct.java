package ru.yandex.practicum.commerce.warehouse.model;

import jakarta.persistence.*;
import lombok.*;

import java.util.UUID;

@Entity
@Table(name = "warehouse_products")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WarehouseProduct {

    @Id
    private UUID productId;  // ID товара из shopping-store

    @Column(name = "quantity", nullable = false)
    private Integer quantity;

    @Column(name = "width")
    private Double width;

    @Column(name = "height")
    private Double height;

    @Column(name = "depth")
    private Double depth;

    @Column(name = "weight")
    private Double weight;

    @Column(name = "fragile")
    private Boolean fragile;
}