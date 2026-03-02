package ru.yandex.practicum.commerce.warehouse;

import lombok.Data;
import java.util.Map;
import java.util.UUID;

@Data
public class AssemblyRequest {
    private UUID orderId;
    private Map<UUID, Long> products; // productId -> quantity
}
