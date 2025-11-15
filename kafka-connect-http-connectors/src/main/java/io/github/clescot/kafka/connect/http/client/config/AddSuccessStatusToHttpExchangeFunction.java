package io.github.clescot.kafka.connect.http.client.config;

import io.github.clescot.kafka.connect.http.core.HttpExchange;

import java.util.function.UnaryOperator;
import java.util.regex.Pattern;


public record AddSuccessStatusToHttpExchangeFunction(
        Pattern successResponseCodeRegex) implements UnaryOperator<HttpExchange> {

    @Override
    public HttpExchange apply(HttpExchange httpExchange) {
        httpExchange.setSuccess(isSuccess(httpExchange));
        return httpExchange;
    }

    private boolean isSuccess(HttpExchange httpExchange) {
        return successResponseCodeRegex.matcher(httpExchange.getResponse().getStatusCode() + "").matches();
    }

    @Override
    public String toString() {
        return "{" +
                "successResponseCodeRegex=" + successResponseCodeRegex +
                '}';
    }
}
