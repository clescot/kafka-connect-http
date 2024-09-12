package io.github.clescot.kafka.connect.http.source;

public class CronException extends RuntimeException{
    public CronException() {
    }

    public CronException(Throwable cause) {
        super(cause);
    }

    public CronException(String message) {
        super(message);
    }

    public CronException(String message, Throwable cause) {
        super(message, cause);
    }

    public CronException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
