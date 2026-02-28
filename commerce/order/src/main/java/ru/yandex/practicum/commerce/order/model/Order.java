package ru.yandex.practicum.commerce.order.model;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.commerce.dto.enums.OrderState;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "orders")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID orderId;

    @Column(nullable = false)
    private UUID shoppingCartId;

    @ElementCollection
    @CollectionTable(name = "order_products",
            joinColumns = @JoinColumn(name = "order_id"))
    @MapKeyColumn(name = "product_id")
    @Column(name = "quantity")
    private Map<UUID, Long> products;

    @Enumerated(EnumType.STRING)
    private OrderState state;

    private Double totalPrice;
    private Double productsPrice;
    private Double deliveryPrice;
    private Double volume;
    private Double weight;
    private Boolean fragile;

    private UUID deliveryId;
    private UUID paymentId;
}