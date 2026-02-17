package ru.yandex.practicum.commerce.cart;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.http.HttpStatus;

@Data
@EqualsAndHashCode(callSuper = true)
public class NotAuthorizedUserException extends RuntimeException {
    private final HttpStatus httpStatus;
    private final String userMessage;

    public NotAuthorizedUserException(String message) {
        super(message);
        this.httpStatus = HttpStatus.UNAUTHORIZED;
        this.userMessage = "Пользователь не авторизован";
    }

    public NotAuthorizedUserException(String message, String userMessage) {
        super(message);
        this.httpStatus = HttpStatus.UNAUTHORIZED;
        this.userMessage = userMessage;
    }
}