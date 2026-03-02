package ru.yandex.practicum.commerce.order.model;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.commerce.dto.enums.OrderState;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "orders")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID orderId;

    @Column(nullable = false)
    private String shoppingCartId;

    @ElementCollection
    @CollectionTable(name = "order_products",
            joinColumns = @JoinColumn(name = "order_id"))
    @MapKeyColumn(name = "product_id")
    @Column(name = "quantity")
    private Map<UUID, Long> products;

    @Enumerated(EnumType.STRING)
    private OrderState state;
    private BigDecimal totalPrice;
    private BigDecimal productsPrice;
    private BigDecimal deliveryPrice;
    private Double volume;
    private Double weight;
    private Boolean fragile;

    private UUID deliveryId;
    private UUID paymentId;
}