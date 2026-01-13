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

    @Override
    public <R extends Request,S extends Response,E extends Exchange<R,S>> Exchange<R, S> process(@NotNull E exchange, @NotNull String template, Map<String, Object> context) {
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
                    if (exchange.getAttributes().containsKey(MOMENT)) {
                        Object moment = exchange.getAttributes().get(MOMENT);
                        if (moment instanceof String) {
                            try {
                                ZonedDateTime zonedDateTime = ZonedDateTime.parse((String) moment);
                                dateTimeString = zonedDateTime.format(formatter);
                            } catch (DateTimeParseException e) {
                                LOGGER.warn("Failed to parse moment '{}': {}", moment, e.getMessage());
                                dateTimeString = "";
                            }
                        } else {
                            dateTimeString = moment.toString();
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
    public boolean supports(@NotNull String template) {
        return template.startsWith("${datetime:") && template.contains(":");
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
        
        // Handle special cases for known sources
        if (innerContent.startsWith(NOW + ":")) {
            // ${datetime:now:format} or ${datetime:now:format:attribute}
            String[] parts = innerContent.split(":", 3);
            return parts;
        } else if (innerContent.startsWith(CURRENT + ":")) {
            // ${datetime:current:format} or ${datetime:current:format:attribute}
            String[] parts = innerContent.split(":", 3);
            return parts;
        } else if (innerContent.startsWith(MOMENT + ":")) {
            // ${datetime:moment:format} or ${datetime:moment:format:attribute}
            String[] parts = innerContent.split(":", 3);
            return parts;
        } else if (innerContent.startsWith(EPOCH)) {
            // ${datetime:epoch} or ${datetime:epoch:format} or ${datetime:epoch:format:attribute}
            String[] parts = innerContent.split(":", 3);
            return parts;
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
}