package io.github.clescot.kafka.connect.http.client.config;

import io.github.clescot.kafka.connect.http.core.HttpExchange;

import java.util.function.UnaryOperator;
import java.util.regex.Pattern;


public class AddSuccessStatusToHttpExchangeFunction implements UnaryOperator<HttpExchange> {

    private final Pattern successResponseCodeRegex;

    public AddSuccessStatusToHttpExchangeFunction(Pattern successResponseCodeRegex) {
        this.successResponseCodeRegex = successResponseCodeRegex;
    }

    @Override
    public HttpExchange apply(HttpExchange httpExchange) {
        httpExchange.setSuccess(isSuccess(httpExchange));
        return httpExchange;
    }

    protected boolean isSuccess(HttpExchange httpExchange) {
        Pattern pattern = this.getSuccessResponseCodeRegex();
        boolean success = pattern.matcher(httpExchange.getResponse().getStatusCode() + "").matches();
        return success;
    }


    public Pattern getSuccessResponseCodeRegex() {
        return successResponseCodeRegex;
    }

    @Override
    public String toString() {
        return "{" +
                "successResponseCodeRegex=" + successResponseCodeRegex +
                '}';
    }
}
