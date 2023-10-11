package io.github.clescot.kafka.connect.http.client;


import io.github.clescot.kafka.connect.http.core.HttpExchange;

/**
 * exception for handling retry policy, when a WS Server error occurs.
 * i.e, retry occured only when a technical error is thrown.
 * A functional error, like a weird parameter will not be retried.
 */
public class HttpException extends RuntimeException {

    private final HttpExchange httpExchange;

    public HttpException() {
        httpExchange = null;
    }
    public HttpException(HttpExchange httpExchange, String message) {
        super(message);
        this.httpExchange = httpExchange;
    }
    public HttpException(String message) {
        super(message);
        httpExchange = null;
    }

    public HttpException(String message, Throwable cause) {
        super(message, cause);
        httpExchange = null;
    }

    public HttpException(Throwable cause) {
        super(cause);
        httpExchange = null;
    }

    public HttpException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
        httpExchange = null;
    }

    public HttpExchange getHttpExchange() {
        return httpExchange;
    }
}
