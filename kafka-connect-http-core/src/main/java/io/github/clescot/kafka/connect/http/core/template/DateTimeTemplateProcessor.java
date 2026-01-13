package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.core.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;

/**
 * Date/Time template processor for formatting and parsing dates and times.
 * Supports various date/time sources and output formats.
 */
public class DateTimeTemplateProcessor implements ExchangeTemplateProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DateTimeTemplateProcessor.class);
    public static final String MOMENT = "moment";
    public static final String CURRENT = "current";
    public static final String NOW = "now";
    public static final String EPOCH = "epoch";
    public static final String ISO_8601_FORMAT_IN_UTC = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    public static final String ATTRIBUTE_NAME_PATTERN = "^[a-zA-Z_][a-zA-Z0-9_]*$";

    @Override
    public <R extends Request,S extends Response,E extends Exchange<R,S>> Exchange<R, S> process(E exchange, String template, Map<String, Object> context) {
        if (exchange == null) {
            LOGGER.warn("Exchange parameter is null");
            throw new NullPointerException("Exchange parameter cannot be null");
        }
        if (template == null) {
            LOGGER.warn("Template parameter is null");
            return exchange.withAttribute("formatted_datetime", "");
        }
        
        // Extract parts from template: ${datetime:source:format:attributeName}
        String[] parts = extractTemplateParts(template);
        String source = parts.length > 0 ? parts[0] : "";
        String format = parts.length > 1 ? parts[1] : ISO_8601_FORMAT_IN_UTC;
        String attributeName = parts.length > 2 ? parts[2] : "formatted_datetime";
        
        String dateTimeString = "";
        
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
            
            // Determine the source of the datetime
            switch (source.toLowerCase()) {
                case NOW:
                case CURRENT:
                    dateTimeString = ZonedDateTime.now().format(formatter);
                    break;
                case MOMENT:
                    // Use the exchange's attributes for moment
                    Map<String, Object> attributes = exchange.getAttributes();
                    if (attributes != null && attributes.containsKey(MOMENT)) {
                        Object moment = attributes.get(MOMENT);
                        if (moment != null) {
                            if (moment instanceof String momentString) {
                                try {
                                    ZonedDateTime zonedDateTime = ZonedDateTime.parse(momentString);
                                    dateTimeString = zonedDateTime.format(formatter);
                                } catch (DateTimeParseException e) {
                                    // If parsing fails, use the moment string as-is
                                    LOGGER.warn("Failed to parse moment '{}': {}, using as-is", moment, e.getMessage());
                                    dateTimeString = momentString;
                                }
                            } else {
                                // For non-string objects, use toString()
                                dateTimeString = moment.toString();
                            }
                        } else {
                            dateTimeString = "";
                        }
                    } else {
                        dateTimeString = "";
                    }
                    break;
                case EPOCH:
                    // For epoch, if no custom format is specified, return raw epoch timestamp
                    if (format.equals(ISO_8601_FORMAT_IN_UTC)) {
                        dateTimeString = String.valueOf(System.currentTimeMillis());
                    } else {
                        // Current epoch time formatted with the specified pattern
                        Instant now = Instant.now();
                        ZonedDateTime epochZonedDateTime = now.atZone(ZoneId.systemDefault());
                        dateTimeString = epochZonedDateTime.format(formatter);
                    }
                    break;
                default:
                    // Try to parse the source as a timestamp
                    try {
                        if (source.matches("\\d+")) {
                            // Epoch time
                            long epochMillis = Long.parseLong(source);
                            Instant instant = Instant.ofEpochMilli(epochMillis);
                            ZonedDateTime parsedZonedDateTime = instant.atZone(ZoneId.systemDefault());
                            dateTimeString = parsedZonedDateTime.format(formatter);
                        } else {
                            // Try to parse as ISO date
                            ZonedDateTime parsedZonedDateTime = ZonedDateTime.parse(source);
                            dateTimeString = parsedZonedDateTime.format(formatter);
                        }
                    } catch (Exception e) {
                        LOGGER.warn("Failed to parse datetime source '{}': {}", source, e.getMessage());
                        dateTimeString = "";
                    }
            }
            
            LOGGER.debug("Formatted datetime '{}' with pattern '{}': {}", source, format, dateTimeString);
            
        } catch (Exception e) {
            LOGGER.warn("Failed to process datetime template '{}': {}", template, e.getMessage());
            dateTimeString = "";
        }
        
        // Always return the exchange with the attribute
        return exchange.withAttribute(attributeName, dateTimeString);
    }
    
    @Override
    public boolean supports(String template) {
        return template != null && 
               template.startsWith("${datetime:") && 
               template.contains(":") && 
               template.endsWith("}");
    }
    
    @Override
    public String getName() {
        return "datetime";
    }
    
    /**
     * Extract parts from template: ${datetime:source:format:attributeName}
     * Returns array where [0] = source, [1] = format (if present), [2] = attributeName (if present)
     */
    private String[] extractTemplateParts(String template) {
        // Remove ${datetime: and }
        String innerContent = template.substring("${datetime:".length(), template.length() - 1);
        
        // For known sources, we need to handle them specially to avoid confusion with custom sources
        if (innerContent.startsWith(NOW + ":")) {
            // Special handling for "now" to preserve the original logic for this common case
            return parseKnownSource(innerContent, NOW);
        } else if (innerContent.startsWith(CURRENT + ":")) {
            return parseKnownSource(innerContent, CURRENT);
        } else if (innerContent.startsWith(MOMENT + ":")) {
            return parseKnownSource(innerContent, MOMENT);
        } else if (innerContent.startsWith(EPOCH)) {
            if (innerContent.equals(EPOCH)) {
                return new String[]{EPOCH};
            } else {
                return parseKnownSource(innerContent, EPOCH);
            }
        } else {
            // For custom sources (like ISO dates with colons), we need to be more careful
            // Find the last colon to separate format from source
            int lastColonIndex = innerContent.lastIndexOf(":");
            if (lastColonIndex == -1) {
                // No format or attribute name, just source
                return new String[]{innerContent};
            }
            
            String source = innerContent.substring(0, lastColonIndex);
            String remainder = innerContent.substring(lastColonIndex + 1);
            
            // Check if there's an attribute name (another colon)
            int secondLastColonIndex = remainder.lastIndexOf(":");
            if (secondLastColonIndex == -1) {
                // Only format, no attribute name
                return new String[]{source, remainder};
            } else {
                // Both format and attribute name
                String format = remainder.substring(0, secondLastColonIndex);
                String attributeName = remainder.substring(secondLastColonIndex + 1);
                return new String[]{source, format, attributeName};
            }
        }
    }
    
    /**
     * Helper method to parse templates with known sources, handling format patterns that contain colons.
     */
    private String[] parseKnownSource(String innerContent, String source) {
        String remainder = innerContent.substring((source + ":").length());
        
        // Check if the remainder looks like it has an attribute name
        // Heuristic: if the part after the last colon doesn't contain common date format characters,
        // treat it as an attribute name, otherwise treat the whole thing as a format
        int lastColonIndex = remainder.lastIndexOf(":");
        if (lastColonIndex == -1) {
            // No colon, so it's just a format
            return new String[]{source, remainder};
        } else {
            String possibleAttributeName = remainder.substring(lastColonIndex + 1);
            // Better heuristic: if the possible attribute name looks like a simple identifier 
            // (alphanumeric with maybe underscores) and is reasonably short, treat it as attribute name
            // Otherwise, if it contains spaces, timezone patterns, or other format-like elements, treat as format
            if (possibleAttributeName.matches(ATTRIBUTE_NAME_PATTERN) && possibleAttributeName.length() <= 50) {
                // Looks like a simple attribute name
                String format = remainder.substring(0, lastColonIndex);
                String attributeName = possibleAttributeName;
                return new String[]{source, format, attributeName};
            } else {
                // Contains spaces, special characters, or is too long - likely part of format
                return new String[]{source, remainder};
            }
        }
    }
}