package ru.yandex.practicum.commerce.cart.exception;

public class CartNotActiveException extends RuntimeException {

    public CartNotActiveException(String message) {
        super(message);
    }
}
