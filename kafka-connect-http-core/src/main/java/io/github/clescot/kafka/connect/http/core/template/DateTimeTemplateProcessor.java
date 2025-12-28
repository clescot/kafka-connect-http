package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
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
    
    private static final Logger log = LoggerFactory.getLogger(DateTimeTemplateProcessor.class);
    
    @Override
    public Exchange<?, ?> process(@NotNull Exchange<?, ?> exchange, @NotNull String template, Map<String, Object> context) {
        try {
            // Extract parts from template: ${datetime:source:format:attributeName}
            String[] parts = extractTemplateParts(template);
            if (parts.length < 2) {
                log.warn("Invalid datetime template format: {}", template);
                return exchange;
            }
            
            String source = parts[0];
            String format = parts.length > 1 ? parts[1] : "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
            String attributeName = parts.length > 2 ? parts[2] : "formatted_datetime";
            
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
            String dateTimeString;
            
            // Determine the source of the datetime
            switch (source.toLowerCase()) {
                case "now":
                case "current":
                    dateTimeString = ZonedDateTime.now().format(formatter);
                    break;
                case "moment":
                    // Use the exchange's moment/timestamp
                    if (exchange.getMetadata().containsKey("moment")) {
                        Object moment = exchange.getMetadata().get("moment");
                        if (moment instanceof String) {
                            try {
                                ZonedDateTime zonedDateTime = ZonedDateTime.parse((String) moment);
                                dateTimeString = zonedDateTime.format(formatter);
                            } catch (DateTimeParseException e) {
                                log.warn("Failed to parse moment '{}': {}", moment, e.getMessage());
                                dateTimeString = "";
                            }
                        } else {
                            dateTimeString = moment.toString();
                        }
                    } else {
                        dateTimeString = "";
                    }
                    break;
                case "epoch":
                    // Current epoch time
                    dateTimeString = String.valueOf(System.currentTimeMillis());
                    break;
                default:
                    // Try to parse the source as a timestamp
                    try {
                        if (source.matches("\\d+")) {
                            // Epoch time
                            long epochMillis = Long.parseLong(source);
                            Instant instant = Instant.ofEpochMilli(epochMillis);
                            ZonedDateTime zonedDateTime = instant.atZone(ZoneId.systemDefault());
                            dateTimeString = zonedDateTime.format(formatter);
                        } else {
                            // Try to parse as ISO date
                            ZonedDateTime zonedDateTime = ZonedDateTime.parse(source);
                            dateTimeString = zonedDateTime.format(formatter);
                        }
                    } catch (Exception e) {
                        log.warn("Failed to parse datetime source '{}': {}", source, e.getMessage());
                        dateTimeString = "";
                    }
            }
            
            log.debug("Formatted datetime '{}' with pattern '{}': {}", source, format, dateTimeString);
            return exchange.withAttribute(attributeName, dateTimeString);
            
        } catch (Exception e) {
            log.warn("Failed to process datetime template '{}': {}", template, e.getMessage());
            return exchange;
        }
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
        
        // Split by colons
        String[] parts = innerContent.split(":", 3);
        
        return parts;
    }
}