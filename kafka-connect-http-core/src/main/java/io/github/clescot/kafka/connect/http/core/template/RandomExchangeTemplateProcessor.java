package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.core.Response;

/**
 * Random value generator template processor for Exchange.
 * Allows generating random values and adding them to any Exchange implementation.
 */
public class RandomExchangeTemplateProcessor implements ExchangeTemplateProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(RandomExchangeTemplateProcessor.class);
    
    public static final String NAME = "random";
    private static final Pattern RANDOM_PATTERN = Pattern.compile("\\$\\{random\\.(\\w+)(?::(\\d+))?(?::(\\d+))?\\}");
    private static final Random RANDOM = new Random();

    @Override
    public <R extends Request,S extends Response,E extends Exchange<R,S>> Exchange<R, S> process(@NotNull E exchange, @NotNull String template, Map<String, Object> context) {
        LOGGER.debug("Processing template with random values: {}", template);
        
        // Start with the original exchange
        Exchange<R, S> modifiedExchange = exchange;
        
        // Process the template to find random value expressions
        Matcher matcher = RANDOM_PATTERN.matcher(template);
        
        while (matcher.find()) {
            String type = matcher.group(1); // type of random value
            String minStr = matcher.group(2); // optional min value
            String maxStr = matcher.group(3); // optional max value
            
            LOGGER.debug("Found random expression: type={}, min={}, max={}", type, minStr, maxStr);
            
            try {
                Object randomValue = generateRandomValue(type, minStr, maxStr);
                if (randomValue != null) {
                    String attributeName = "random_" + type + "_" + UUID.randomUUID().toString().replace("-", "");
                    modifiedExchange = modifiedExchange.withAttribute(attributeName, randomValue.toString());
                    LOGGER.debug("Generated random value {}: {}", attributeName, randomValue);
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to generate random value of type '{}': {}", type, e.getMessage());
            }
        }
        
        return modifiedExchange;
    }

    /**
     * Generate a random value based on the specified type and range.
     * 
     * @param type the type of random value (int, long, double, uuid, string, boolean)
     * @param minStr optional minimum value as string
     * @param maxStr optional maximum value as string
     * @return the generated random value
     */
    private Object generateRandomValue(String type, String minStr, String maxStr) {
        try {
            switch (type.toLowerCase()) {
                case "int":
                case "integer":
                    int minInt = minStr != null ? Integer.parseInt(minStr) : 0;
                    int maxInt = maxStr != null ? Integer.parseInt(maxStr) : 100;
                    return RANDOM.nextInt(maxInt - minInt + 1) + minInt;
                    
                case "long":
                    long minLong = minStr != null ? Long.parseLong(minStr) : 0L;
                    long maxLong = maxStr != null ? Long.parseLong(maxStr) : 1000L;
                    return minLong + (long) (RANDOM.nextDouble() * (maxLong - minLong + 1));
                    
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

    /**
     * Generate a random alphanumeric string.
     * 
     * @param length the length of the string to generate
     * @return the generated random string
     */
    private String generateRandomString(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(characters.charAt(RANDOM.nextInt(characters.length())));
        }
        return sb.toString();
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