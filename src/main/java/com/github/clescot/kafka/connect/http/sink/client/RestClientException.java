package com.github.clescot.kafka.connect.http.sink.client;

/**
 * exception for handling retry policy, when a WS Server error occurs.
 * i.e, retry occured only when a technical error is thrown.
 * A functional error, like a weird parameter will not be retried.
 */
public class RestClientException extends RuntimeException {

    public RestClientException() {
    }

    public RestClientException(String message) {
        super(message);
    }

    public RestClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public RestClientException(Throwable cause) {
        super(cause);
    }

    public RestClientException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
