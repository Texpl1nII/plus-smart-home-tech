package ru.yandex.practicum.telemetry.collector.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidationExceptions(
            MethodArgumentNotValidException ex) {

        Map<String, String> errors = new HashMap<>();
        ex.getBindingResult().getAllErrors().forEach((error) -> {
            String fieldName = ((FieldError) error).getField();
            String errorMessage = error.getDefaultMessage();
            errors.put(fieldName, errorMessage);
        });

        Map<String, Object> response = new HashMap<>();
        response.put("error", "Validation failed");
        response.put("errors", errors);
        response.put("timestamp", Instant.now().toString());
        response.put("status", 400);

        log.warn("Validation failed: {}", errors);
        return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);  // Исправлено
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgument(IllegalArgumentException ex) {
        Map<String, Object> response = new HashMap<>();
        response.put("error", "Invalid request");
        response.put("message", ex.getMessage());
        response.put("timestamp", Instant.now().toString());
        response.put("status", 400);

        log.warn("Invalid argument: {}", ex.getMessage());
        return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);  // Исправлено
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGeneralException(Exception ex) {
        Map<String, Object> response = new HashMap<>();
        response.put("error", "Internal server error");
        response.put("message", ex.getMessage());
        response.put("timestamp", Instant.now().toString());
        response.put("status", 500);

        log.error("Internal server error", ex);
        return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);  // Исправлено
    }
}
