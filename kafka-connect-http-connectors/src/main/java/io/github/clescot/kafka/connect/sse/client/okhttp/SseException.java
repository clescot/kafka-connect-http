package io.github.clescot.kafka.connect.sse.client.okhttp;

public class SseException extends RuntimeException{
    public SseException() {
    }

    public SseException(Throwable cause) {
        super(cause);
    }

    public SseException(String message) {
        super(message);
    }

    public SseException(String message, Throwable cause) {
        super(message, cause);
    }

    public SseException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
