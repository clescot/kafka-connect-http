package io.github.clescot.kafka.connect.http.client;

import io.github.clescot.kafka.connect.http.core.HttpExchange;

public class RetryException extends RuntimeException {

    private final HttpExchange httpExchange;

    public RetryException() {
        httpExchange = null;
    }
    public RetryException(HttpExchange httpExchange, String message) {
        super(message);
        this.httpExchange = httpExchange;
    }
    public RetryException(String message) {
        super(message);
        httpExchange = null;
    }

    public RetryException(String message, Throwable cause) {
        super(message, cause);
        httpExchange = null;
    }

    public RetryException(Throwable cause) {
        super(cause);
        httpExchange = null;
    }

    public RetryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
        httpExchange = null;
    }

    public HttpExchange getHttpExchange() {
        return httpExchange;
    }
}
