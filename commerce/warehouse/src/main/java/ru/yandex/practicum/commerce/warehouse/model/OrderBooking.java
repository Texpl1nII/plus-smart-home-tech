package ru.yandex.practicum.commerce.warehouse.model;

import jakarta.persistence.*;
import lombok.*;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "order_bookings")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderBooking {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID bookingId;

    @Column(nullable = false, unique = true)
    private UUID orderId;

    private UUID deliveryId;

    @ElementCollection
    @CollectionTable(name = "booking_products",
            joinColumns = @JoinColumn(name = "booking_id"))
    @MapKeyColumn(name = "product_id")
    @Column(name = "quantity")
    private Map<UUID, Long> products;

    @Column(nullable = false)
    private Boolean assembled;

    @Column(nullable = false)
    private Boolean shipped;
}
