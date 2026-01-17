package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.*;
import io.github.clescot.kafka.connect.sse.core.SseExchange;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Hashing template processor for generating cryptographic hashes.
 * Supports various hash algorithms like MD5, SHA-256, etc.
 */
public class HashingTemplateProcessor implements ExchangeTemplateProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(HashingTemplateProcessor.class);
    private static final Set<String> SUPPORTED_ALGORITHMS = Set.of(
            "MD5", "SHA-1", "SHA-256", "SHA-384", "SHA-512",
            "SHA3-256", "SHA3-384", "SHA3-512"
    );
    public static final String NAME = "hash";

    @Override
    public <R extends Request, S extends Response, E extends Exchange<R, S>> E process(
            @NotNull E exchange, @NotNull String template, Map<String, Object> context) {

        try {
            String[] parts = extractTemplateParts(template);
            if (parts.length < 2) {
                LOGGER.warn("Invalid hash template format: {}", template);
                return exchange;
            }

            String algorithm = parts[0].toUpperCase();
            String input = parts[1];

            if (!SUPPORTED_ALGORITHMS.contains(algorithm)) {
                LOGGER.warn("Unsupported hash algorithm '{}' in template '{}'", algorithm, template);
                return exchange;
            }

            if (input == null || input.isEmpty()) {
                LOGGER.debug("Empty input for hashing");
                return (E) setContent(exchange, "");
            }

            String hash = generateHash(algorithm, input);

            LOGGER.debug("Generated {} hash for input: {}", algorithm, hash);
            return (E) setContent(exchange, hash);

        } catch (NoSuchAlgorithmException e) {
            LOGGER.warn("Invalid hash algorithm in template '{}': {}", template, e.getMessage());
            return exchange;
        } catch (Exception e) {
            LOGGER.warn("Failed to process hash template '{}': {}", template, e.getMessage());
            return exchange;
        }
    }

    private String[] extractTemplateParts(String template) {
        if (!template.startsWith("${hash:") || !template.endsWith("}")) {
            return new String[0];
        }
        String innerContent = template.substring("${hash:".length(), template.length() - 1);
        String[] parts = innerContent.split(":", 2);
        return parts.length >= 2 ? parts : new String[]{parts[0], ""};
    }

    private String generateHash(String algorithm, String input) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance(algorithm);
        byte[] hashBytes = digest.digest(input.getBytes());

        BigInteger number = new BigInteger(1, hashBytes);
        String hexString = number.toString(16);

        int expectedLength = hashBytes.length * 2;
        StringBuilder paddedHex = new StringBuilder(hexString);
        while (paddedHex.length() < expectedLength) {
            paddedHex.insert(0, '0');
        }

        return paddedHex.toString();
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
        if (template == null || !template.startsWith("${hash:") || !template.endsWith("}")) {
            return false;
        }
        String innerContent = template.substring("${hash:".length(), template.length() - 1);
        String[] parts = innerContent.split(":", 2);
        if (parts.length < 1 || parts[0].isEmpty()) {
            return false;
        }
        String algorithm = parts[0].toUpperCase();
        return SUPPORTED_ALGORITHMS.contains(algorithm);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
