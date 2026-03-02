package ru.yandex.practicum.commerce.warehouse;

import lombok.Data;
import java.util.Map;
import java.util.UUID;

@Data
public class ReturnRequest {
    private UUID orderId;
    private Map<UUID, Long> products; // возвращаемые товары
}
