package ru.yandex.practicum.commerce.warehouse.exceptions;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.http.HttpStatus;

import java.util.UUID;

@Data
@EqualsAndHashCode(callSuper = true)
public class SpecifiedProductAlreadyInWarehouseException extends RuntimeException {
    private final HttpStatus httpStatus;
    private final String userMessage;

    public SpecifiedProductAlreadyInWarehouseException(UUID productId) {
        super("Product already exists in warehouse: " + productId);
        this.httpStatus = HttpStatus.BAD_REQUEST;
        this.userMessage = "Товар с ID " + productId + " уже зарегистрирован на складе";
    }
}
