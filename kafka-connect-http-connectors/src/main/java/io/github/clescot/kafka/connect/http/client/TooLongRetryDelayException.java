package io.github.clescot.kafka.connect.http.client;

public class TooLongRetryDelayException extends RuntimeException {

    public TooLongRetryDelayException(long secondsToWait, long retryDelayThreshold) {
        super("The retry delay is too long: " + secondsToWait + " seconds. The threshold is " + retryDelayThreshold + " seconds.");
    }

    public TooLongRetryDelayException(String message) {
        super(message);
    }

    public TooLongRetryDelayException(String message, Throwable cause) {
        super(message, cause);
    }

    public TooLongRetryDelayException(Throwable cause) {
        super(cause);
    }

    public TooLongRetryDelayException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
