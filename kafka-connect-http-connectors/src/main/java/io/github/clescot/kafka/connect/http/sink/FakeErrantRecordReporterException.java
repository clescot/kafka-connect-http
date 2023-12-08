package io.github.clescot.kafka.connect.http.sink;

import io.github.clescot.kafka.connect.http.core.HttpExchange;

public class FakeErrantRecordReporterException extends RuntimeException{

    private HttpExchange httpExchange;
    public FakeErrantRecordReporterException() {
    }

    public FakeErrantRecordReporterException(String message) {
        super(message);
    }

    public FakeErrantRecordReporterException(String message, Throwable cause) {
        super(message, cause);
    }

    public FakeErrantRecordReporterException(Throwable cause) {
        super(cause);
    }

    public FakeErrantRecordReporterException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
    public FakeErrantRecordReporterException(HttpExchange httpExchange) {
        this.httpExchange = httpExchange;
    }
}
