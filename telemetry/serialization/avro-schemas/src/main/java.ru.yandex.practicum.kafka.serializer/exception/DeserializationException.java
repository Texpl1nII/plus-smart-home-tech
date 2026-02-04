package exception;

public class DeserializationException extends RuntimeException {
    public DeserializationException(String message, Throwable ex) {
        super(message, ex);
    }
}
