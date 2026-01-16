package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.core.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
    public <R extends Request,S extends Response,E extends Exchange<R,S>> E process(@NotNull E exchange, @NotNull String template, Map<String, Object> context) {
        String algorithm = null;
        String attributeName = null;
        
        try {
            // Extract parts from template: ${hash:algorithm:input:attributeName}
            String[] parts = extractTemplateParts(template);
            if (parts.length < 2) {
                LOGGER.warn("Invalid hash template format: {}", template);
                return   exchange;
            }
            
            algorithm = parts[0].toUpperCase();
            String input = parts[1];
            attributeName = parts.length > 2 ? parts[2] : "hash_result";
            
            // Get the input value (could be literal or attribute reference)
            String inputValue = getInputValue(input, exchange);
            if (inputValue == null || inputValue.isEmpty()) {
                LOGGER.debug("Empty input for hashing");
                return exchange.withAttribute(attributeName, "");
            }
            
            // Generate hash
            String hash = generateHash(algorithm, inputValue);
            
            LOGGER.debug("Generated {} hash for input: {}", algorithm, hash);
            return exchange.withAttribute(attributeName, hash);
            
        } catch (NoSuchAlgorithmException e) {
            LOGGER.warn("Invalid hash algorithm '{}' in template '{}': {}", algorithm, template, e.getMessage());
            return attributeName != null ? exchange.withAttribute(attributeName, "") : exchange;
        } catch (Exception e) {
            LOGGER.warn("Failed to process hash template '{}': {}", template, e.getMessage());
            return exchange;
        }
    }
    
    @Override
    public boolean supports(@NotNull String template) {
        if (!template.startsWith("${hash:") || !template.contains(":")) {
            return false;
        }
        String[] parts = extractTemplateParts(template);
        if (parts.length < 2) {
            return false;
        }
        String algorithm = parts[0].toUpperCase();
        return SUPPORTED_ALGORITHMS.contains(algorithm);
    }
    
    @Override
    public String getName() {
        return NAME;
    }
    
    /**
     * Extract parts from template: ${hash:algorithm:input:attributeName}
     */
    private String[] extractTemplateParts(String template) {
        // Remove ${hash: and }
        String innerContent = template.substring("${hash:".length(), template.length() - 1);
        
        // Split by colons
        String[] parts = innerContent.split(":", 3);
        
        if (parts.length >= 2) {
            return parts;
        } else {
            return new String[]{parts[0], ""};
        }
    }
    
    /**
     * Get input value - could be literal or attribute reference
     */
    private String getInputValue(String input, Exchange<?, ?> exchange) {
        // If input starts with @, it's an attribute reference
        if (input.startsWith("@")) {
            String attrName = input.substring(1);
            Object attrValue = exchange.getAttribute(attrName);
            return attrValue != null ? attrValue.toString() : "";
        }
        // Otherwise, it's a literal value
        return input;
    }
    
    /**
     * Generate hash using specified algorithm
     */
    private String generateHash(String algorithm, String input) throws NoSuchAlgorithmException {
        if (!SUPPORTED_ALGORITHMS.contains(algorithm)) {
            throw new NoSuchAlgorithmException("Unsupported hash algorithm: " + algorithm);
        }
        MessageDigest digest = MessageDigest.getInstance(algorithm);
        byte[] hashBytes = digest.digest(input.getBytes());
        
        // Convert to hexadecimal string
        BigInteger number = new BigInteger(1, hashBytes);
        String hexString = number.toString(16);
        
        // Pad with leading zeros if needed (each byte = 2 hex chars)
        int expectedLength = hashBytes.length * 2;
        StringBuilder paddedHex = new StringBuilder(hexString);
        while (paddedHex.length() < expectedLength) {
            paddedHex.insert(0, '0');
        }
        
        return paddedHex.toString();
    }
}