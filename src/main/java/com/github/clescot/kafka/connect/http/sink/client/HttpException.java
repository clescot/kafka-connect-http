package com.github.clescot.kafka.connect.http.sink.client;

/**
 * exception for handling retry policy, when a WS Server error occurs.
 * i.e, retry occured only when a technical error is thrown.
 * A functional error, like a weird parameter will not be retried.
 */
public class HttpException extends RuntimeException {

    public HttpException() {
    }

    public HttpException(String message) {
        super(message);
    }

    public HttpException(String message, Throwable cause) {
        super(message, cause);
    }

    public HttpException(Throwable cause) {
        super(cause);
    }

    public HttpException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
