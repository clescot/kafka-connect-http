package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.*;
import io.github.clescot.kafka.connect.sse.core.SseExchange;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Header/Parameter template processor for accessing HTTP headers, query parameters, and cookies.
 */
public class HeaderParameterTemplateProcessor implements ExchangeTemplateProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeaderParameterTemplateProcessor.class);
    public static final String NAME = "headerparam";

    @Override
    public <R extends Request, S extends Response, E extends Exchange<R, S>> E process(
            @NotNull E exchange, @NotNull String template, Map<String, Object> context) {
        try {
            if (!(exchange instanceof HttpExchange)) {
                LOGGER.warn("HeaderParameter processor only works with HttpExchange, got: {}", exchange.getClass().getName());
                return exchange;
            }

            HttpExchange httpExchange = (HttpExchange) exchange;
            HttpRequest request = httpExchange.getRequest();
            HttpResponse response = httpExchange.getResponse();

            String[] parts = extractTemplateParts(template);
            if (parts.length < 2) {
                LOGGER.warn("Invalid header/param template format: {}", template);
                return exchange;
            }

            String type = parts[0];
            String name = parts[1];

            String value = "";

            switch (type.toLowerCase()) {
                case "header":
                    value = getHeaderValue(request, response, name);
                    break;
                case "param":
                    value = getQueryParameter(request, name);
                    break;
                case "cookie":
                    value = getCookieValue(request, name);
                    break;
                default:
                    LOGGER.warn("Unknown header/param type: {}", type);
                    return exchange;
            }

            LOGGER.debug("{} '{}' = '{}'", type, name, value);
            return (E) setContent(exchange, value);

        } catch (Exception e) {
            LOGGER.warn("Failed to process header/param template '{}': {}", template, e.getMessage());
            return exchange;
        }
    }

    private String[] extractTemplateParts(String template) {
        if (!template.startsWith("${")) {
            return new String[0];
        }
        String innerContent = template.substring(2, template.length() - 1);
        String[] parts = innerContent.split(":", 2);

        if (parts.length >= 2) {
            return parts;
        } else {
            return new String[]{parts[0], ""};
        }
    }

    private String getHeaderValue(HttpRequest request, HttpResponse response, String headerName) {
        if (request != null && request.getHeaders() != null) {
            List<String> values = request.getHeaders().get(headerName);
            if (values != null && !values.isEmpty()) {
                return values.get(0);
            }
        }

        if (response != null && response.getHeaders() != null) {
            List<String> values = response.getHeaders().get(headerName);
            if (values != null && !values.isEmpty()) {
                return values.get(0);
            }
        }

        return "";
    }

    private String getQueryParameter(HttpRequest request, String paramName) {
        if (request == null || request.getUrl() == null) {
            return "";
        }

        String url = request.getUrl();
        String queryString = "";

        int questionMarkIndex = url.indexOf('?');
        if (questionMarkIndex >= 0) {
            queryString = url.substring(questionMarkIndex + 1);
        }

        for (String param : queryString.split("&")) {
            String[] keyValue = param.split("=", 2);
            if (keyValue.length == 2 && keyValue[0].equals(paramName)) {
                return keyValue[1];
            }
        }

        return "";
    }

    private String getCookieValue(HttpRequest request, String cookieName) {
        if (request == null || request.getHeaders() == null) {
            return "";
        }

        List<String> cookieHeaders = request.getHeaders().get("Cookie");
        if (cookieHeaders == null || cookieHeaders.isEmpty()) {
            return "";
        }

        String cookieHeader = cookieHeaders.get(0);
        if (cookieHeader == null || cookieHeader.isEmpty()) {
            return "";
        }

        for (String cookie : cookieHeader.split(";")) {
            String[] keyValue = cookie.split("=", 2);
            if (keyValue.length == 2 && keyValue[0].trim().equals(cookieName)) {
                return keyValue[1].trim();
            }
        }

        return "";
    }

    @SuppressWarnings("unchecked")
    private <R extends Request, S extends Response> Exchange<R, S> setContent(
            Exchange<R, S> exchange, String content) {

        if (exchange instanceof HttpExchange httpExchange) {
            HttpRequest request = httpExchange.getRequest();
            HttpResponse originalResponse = httpExchange.getResponse();

            HttpResponse newResponse;
            if (originalResponse != null) {
                newResponse = (HttpResponse) originalResponse.clone();
                newResponse.setBodyAsString(content);
            } else {
                newResponse = new HttpResponse(200, "OK");
                newResponse.setBodyAsString(content);
            }

            return (Exchange<R, S>) HttpExchange.Builder.anHttpExchange()
                    .withHttpRequest(request)
                    .withHttpResponse(newResponse)
                    .withDuration(httpExchange.getDurationInMillis())
                    .at(httpExchange.getMoment())
                    .withAttempts(httpExchange.getAttempts())
                    .withAttributes(new HashMap<>(httpExchange.getAttributes()))
                    .withTimings(new HashMap<>(httpExchange.getTimings()))
                    .build();
        }

        if (exchange instanceof SseExchange) {
            SseExchange sseExchange = (SseExchange) exchange;
            return (Exchange<R, S>) sseExchange.setContent(content);
        }

        LOGGER.warn("Unsupported exchange type: {}. Cannot set content.", exchange.getClass().getName());
        return exchange;
    }

    @Override
    public boolean supports(@NotNull String template) {
        return template != null &&
                (template.startsWith("${header:") ||
                        template.startsWith("${param:") ||
                        template.startsWith("${cookie:"));
    }

    @Override
    public String getName() {
        return NAME;
    }
}
