package ru.yandex.practicum.commerce.payment.model;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.commerce.dto.enums.PaymentStatus;
import java.util.UUID;

@Entity
@Table(name = "payments")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Payment {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID paymentId;

    @Column(nullable = false)
    private UUID orderId;

    private Double productsTotal;
    private Double deliveryTotal;
    private Double total;

    @Enumerated(EnumType.STRING)
    private PaymentStatus status;
}
