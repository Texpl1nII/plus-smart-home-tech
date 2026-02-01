package ru.yandex.practicum.telemetry.analyzer.exception;

public class DeserializationException extends RuntimeException {
    public DeserializationException(String message, Throwable ex) {
        super(message, ex);
    }
}
