package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.core.Response;

/**
 * Hashing template processor for generating cryptographic hashes.
 * Supports various hash algorithms like MD5, SHA-256, etc.
 */
public class HashingTemplateProcessor implements ExchangeTemplateProcessor {
    
    private static final Logger log = LoggerFactory.getLogger(HashingTemplateProcessor.class);
    
    @Override
    public <R extends Request, S extends Response> Exchange<R, S> process(@NotNull Exchange<R, S> exchange, @NotNull String template, Map<String, Object> context) {
        try {
            // Extract parts from template: ${hash:algorithm:input:attributeName}
            String[] parts = extractTemplateParts(template);
            if (parts.length < 2) {
                log.warn("Invalid hash template format: {}", template);
                return   exchange;
            }
            
            String algorithm = parts[0].toUpperCase();
            String input = parts[1];
            String attributeName = parts.length > 2 ? parts[2] : "hash_result";
            
            // Get the input value (could be literal or attribute reference)
            String inputValue = getInputValue(input, exchange);
            if (inputValue == null || inputValue.isEmpty()) {
                log.debug("Empty input for hashing");
                return   exchange.withAttribute(attributeName, "");
            }
            
            // Generate hash
            String hash = generateHash(algorithm, inputValue);
            
            log.debug("Generated {} hash for input: {}", algorithm, hash);
            return   exchange.withAttribute(attributeName, hash);
            
        } catch (Exception e) {
            log.warn("Failed to process hash template '{}': {}", template, e.getMessage());
            return   exchange;
        }
    }
    
    @Override
    public boolean supports(@NotNull String template) {
        return template.startsWith("${hash:") && template.contains(":");
    }
    
    @Override
    public String getName() {
        return "hash";
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
        MessageDigest digest = MessageDigest.getInstance(algorithm);
        byte[] hashBytes = digest.digest(input.getBytes());
        
        // Convert to hexadecimal string
        BigInteger number = new BigInteger(1, hashBytes);
        StringBuilder hexString = new StringBuilder(number.toString(16));
        
        // Pad with leading zeros if needed
        while (hexString.length() < 32) {
            hexString.insert(0, '0');
        }
        
        return hexString.toString();
    }
}