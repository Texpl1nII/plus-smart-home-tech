package ru.yandex.practicum.commerce.warehouse.exceptions;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.http.HttpStatus;

import java.util.Map;
import java.util.UUID;

@Data
@EqualsAndHashCode(callSuper = true)
public class ProductInShoppingCartLowQuantityInWarehouse extends RuntimeException {
    private final HttpStatus httpStatus;
    private final String userMessage;
    private final Map<UUID, Integer> unavailableProducts;

    public ProductInShoppingCartLowQuantityInWarehouse(Map<UUID, Integer> unavailableProducts) {
        super("Not enough products in warehouse: " + unavailableProducts.keySet());
        this.httpStatus = HttpStatus.BAD_REQUEST;
        this.userMessage = "Недостаточно товаров на складе";
        this.unavailableProducts = unavailableProducts;
    }
}
