package ru.yandex.practicum.commerce.store.model;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.commerce.dto.enums.ProductCategory;
import ru.yandex.practicum.commerce.dto.enums.ProductStatus;
import ru.yandex.practicum.commerce.dto.enums.AvailabilityStatus;

import java.util.UUID;

@Entity
@Table(name = "products")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Product {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "name", nullable = false)
    private String name;

    @Column(name = "description", nullable = false, length = 1000)
    private String description;

    @Enumerated(EnumType.STRING)
    @Column(name = "category", nullable = false)
    private ProductCategory category;

    @Column(name = "price", nullable = false)
    private Double price;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private ProductStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "availability", nullable = false)
    private AvailabilityStatus availability;

    @Column(name = "image_url")
    private String imageUrl;

    @Column(name = "quantity", nullable = false)
    private Integer quantity; // точное количество для расчета availability
}
