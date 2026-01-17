package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.*;
import io.github.clescot.kafka.connect.sse.core.SseExchange;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Regex template processor for extracting data from exchange content using regular expressions.
 */
public class RegexExchangeTemplateProcessor implements ExchangeTemplateProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegexExchangeTemplateProcessor.class);

    private static final int MAX_REGEX_LENGTH = 500;
    private static final int MAX_CONTENT_LENGTH = 100000;
    private static final int MAX_REGEX_TIMEOUT_MS = 2000;
    public static final String NAME = "regex";

    @Override
    public <R extends Request, S extends Response, E extends Exchange<R, S>> E process(
            @NotNull E exchange, @NotNull String template, Map<String, Object> context) {
        try {
            String[] parts = extractTemplateParts(template);
            if (parts.length < 1 || parts[0].isEmpty()) {
                LOGGER.warn("Invalid regex template format: {}", template);
                return exchange;
            }

            String regexPattern = parts[0];
            String input = parts.length > 1 ? parts[1] : null;

            if (regexPattern.length() > MAX_REGEX_LENGTH) {
                LOGGER.warn("Regex pattern too long ({} characters), max allowed is {}: {}",
                        regexPattern.length(), MAX_REGEX_LENGTH, regexPattern.substring(0, 100) + "...");
                return exchange;
            }

            if (input == null || input.trim().isEmpty()) {
                input = exchange.getContent();
            }

            if (input == null || input.trim().isEmpty()) {
                LOGGER.debug("No input available for regex processing");
                return (E) setContent(exchange, "");
            }

            if (input.length() > MAX_CONTENT_LENGTH) {
                LOGGER.warn("Input too large for regex processing ({} characters), max allowed is {}: {}",
                        input.length(), MAX_CONTENT_LENGTH, input.substring(0, 100) + "...");
                return exchange;
            }

            Pattern pattern;
            try {
                long startTime = System.currentTimeMillis();
                pattern = Pattern.compile(regexPattern);
                long compilationTime = System.currentTimeMillis() - startTime;

                if (compilationTime > MAX_REGEX_TIMEOUT_MS) {
                    LOGGER.warn("Regex pattern compilation took too long ({}ms), possible ReDoS pattern: {}",
                            compilationTime, regexPattern);
                    return exchange;
                }
            } catch (PatternSyntaxException e) {
                LOGGER.warn("Invalid regex pattern in template '{}': {}", template, e.getMessage());
                return exchange;
            }

            Matcher matcher = pattern.matcher(input);

            String result;
            if (matcher.find()) {
                result = matcher.groupCount() > 0 ? matcher.group(1) : matcher.group(0);
                LOGGER.debug("Regex pattern '{}' matched: {}", regexPattern, result);
            } else {
                result = "";
                LOGGER.debug("Regex pattern '{}' did not match any content", regexPattern);
            }

            return (E) setContent(exchange, result);

        } catch (PatternSyntaxException e) {
            LOGGER.warn("Invalid regex pattern in template '{}': {}", template, e.getMessage());
            return exchange;
        } catch (Exception e) {
            LOGGER.warn("Failed to process regex template '{}': {}", template, e.getMessage());
            return exchange;
        }
    }

    private String[] extractTemplateParts(String template) {
        if (!template.startsWith("${regex:") || !template.endsWith("}")) {
            return new String[0];
        }
        String innerContent = template.substring("${regex:".length(), template.length() - 1);
        int colonIndex = findFirstColon(innerContent);

        if (colonIndex > 0) {
            String pattern = innerContent.substring(0, colonIndex);
            String input = innerContent.substring(colonIndex + 1);
            if (looksLikeJsonPathOrReference(input)) {
                return new String[]{pattern, input};
            }
        }
        return new String[]{innerContent};
    }

    private int findFirstColon(String str) {
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == ':' && (i == 0 || str.charAt(i - 1) != '\\')) {
                return i;
            }
        }
        return -1;
    }

    private boolean looksLikeJsonPathOrReference(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }
        String trimmed = str.trim();
        return trimmed.startsWith("$") ||
               trimmed.startsWith("body") ||
               trimmed.startsWith("attributes") ||
               trimmed.startsWith("request.") ||
               trimmed.startsWith("response.");
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
        return template != null && template.startsWith("${regex:") && template.endsWith("}");
    }

    @Override
    public String getName() {
        return NAME;
    }
}
