package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.*;
import io.github.clescot.kafka.connect.sse.core.SseExchange;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Random value generator template processor for Exchange.
 * Allows generating random values.
 */
public class RandomExchangeTemplateProcessor implements ExchangeTemplateProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(RandomExchangeTemplateProcessor.class);

    public static final String NAME = "random";
    private static final Pattern RANDOM_PATTERN = Pattern.compile("\\$\\{random(?:\\.(\\w+))?(?::(\\d+))?(?::(\\d+))?\\}");
    private static final Random RANDOM = new Random();

    @Override
    public <R extends Request, S extends Response, E extends Exchange<R, S>> E process(
            @NotNull E exchange, @NotNull String template, Map<String, Object> context) {
        LOGGER.debug("Processing template with random values: {}", template);

        Matcher matcher = RANDOM_PATTERN.matcher(template);
        if (!matcher.find()) {
            return exchange;
        }

        String type = matcher.group(1);
        String minStr = matcher.group(2);
        String maxStr = matcher.group(3);

        LOGGER.debug("Found random expression: type={}, min={}, max={}", type, minStr, maxStr);

        try {
            Object randomValue = generateRandomValue(type, minStr, maxStr);
            if (randomValue != null) {
                String result = randomValue.toString();
                LOGGER.debug("Generated random value: {}", result);
                return (E) setContent(exchange, result);
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to generate random value of type '{}': {}", type, e.getMessage());
        }

        return exchange;
    }

    private Object generateRandomValue(String type, String minStr, String maxStr) {
        // Default to "int" if no type specified
        String effectiveType = (type == null || type.isEmpty()) ? "int" : type;
        
        try {
            switch (effectiveType.toLowerCase()) {
                case "int":
                case "integer":
                    int minInt = minStr != null ? Integer.parseInt(minStr) : 0;
                    int maxInt = maxStr != null ? Integer.parseInt(maxStr) : 100;
                    return RANDOM.nextInt(maxInt - minInt + 1) + minInt;

                case "long":
                    long minLong = minStr != null ? Long.parseLong(minStr) : 0L;
                    long maxLong = maxStr != null ? Long.parseLong(maxStr) : 1000L;
                    return generateRandomLong(minLong, maxLong);

                case "double":
                case "float":
                    double minDouble = minStr != null ? Double.parseDouble(minStr) : 0.0;
                    double maxDouble = maxStr != null ? Double.parseDouble(maxStr) : 1.0;
                    return minDouble + (RANDOM.nextDouble() * (maxDouble - minDouble));

                case "uuid":
                    return UUID.randomUUID().toString();

                case "string":
                case "str":
                    int length = minStr != null ? Integer.parseInt(minStr) : 10;
                    return generateRandomString(length);

                case "boolean":
                case "bool":
                    return RANDOM.nextBoolean();

                default:
                    LOGGER.warn("Unknown random type: {}", type);
                    return null;
            }
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid number format in random expression: {}", e.getMessage());
            return null;
        }
    }

    private String generateRandomString(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(characters.charAt(RANDOM.nextInt(characters.length())));
        }
        return sb.toString();
    }

    private long generateRandomLong(long min, long max) {
        if (min > max) {
            throw new IllegalArgumentException("min must be less than or equal to max");
        }
        long range = max - min;
        if (range >= 0) {
            return min + RANDOM.nextLong(range + 1);
        } else {
            while (true) {
                long random = RANDOM.nextLong();
                if (random >= min && random <= max) {
                    return random;
                }
            }
        }
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
    public String getName() {
        return NAME;
    }

    @Override
    public boolean supports(String template) {
        return template != null && RANDOM_PATTERN.matcher(template).find();
    }
}
