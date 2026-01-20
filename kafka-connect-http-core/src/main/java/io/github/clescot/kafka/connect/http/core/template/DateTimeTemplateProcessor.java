package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.*;
import io.github.clescot.kafka.connect.sse.core.SseExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
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
    public static final String NAME = "datetime";

    @Override
    public <R extends Request, S extends Response, E extends Exchange<R, S>> E process(
            E exchange, String template, Map<String, Object> context) {
        if (exchange == null) {
            LOGGER.warn("Exchange parameter is null");
            throw new IllegalArgumentException("Exchange parameter cannot be null");
        }
        if (template == null) {
            return exchange;
        }

        String[] parts = extractTemplateParts(template);
        String source = parts.length > 0 ? parts[0] : "";
        String format = parts.length > 1 ? parts[1] : ISO_8601_FORMAT_IN_UTC;

        String dateTimeString = "";

        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);

            switch (source.toLowerCase()) {
                case NOW:
                case CURRENT:
                    dateTimeString = ZonedDateTime.now().format(formatter);
                    break;
                case MOMENT:
                    Map<String, Object> attributes = exchange.getAttributes();
                    if (attributes != null && attributes.containsKey(MOMENT)) {
                        Object moment = attributes.get(MOMENT);
                        if (moment != null) {
                            if (moment instanceof String momentString) {
                                try {
                                    ZonedDateTime zonedDateTime = ZonedDateTime.parse(momentString);
                                    dateTimeString = zonedDateTime.format(formatter);
                                } catch (DateTimeParseException e) {
                                    LOGGER.warn("Failed to parse moment '{}': {}, using as-is", moment, e.getMessage());
                                    dateTimeString = momentString;
                                }
                            } else {
                                dateTimeString = moment.toString();
                            }
                        }
                    }
                    break;
                case EPOCH:
                    if (format.equals(ISO_8601_FORMAT_IN_UTC)) {
                        dateTimeString = String.valueOf(System.currentTimeMillis());
                    } else {
                        Instant now = Instant.now();
                        ZonedDateTime epochZonedDateTime = now.atZone(ZoneId.systemDefault());
                        dateTimeString = epochZonedDateTime.format(formatter);
                    }
                    break;
                default:
                    try {
                        if (source.matches("\\d+")) {
                            long epochMillis = Long.parseLong(source);
                            Instant instant = Instant.ofEpochMilli(epochMillis);
                            ZonedDateTime parsedZonedDateTime = instant.atZone(ZoneId.systemDefault());
                            dateTimeString = parsedZonedDateTime.format(formatter);
                        } else {
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

        return (E) setContent(exchange, dateTimeString);
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

    private String[] extractTemplateParts(String template) {
        if (!template.startsWith("${datetime:") || !template.endsWith("}")) {
            return new String[0];
        }
        String innerContent = template.substring("${datetime:".length(), template.length() - 1);

        if (innerContent.startsWith(NOW + ":")) {
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
            int lastColonIndex = innerContent.lastIndexOf(":");
            if (lastColonIndex == -1) {
                return new String[]{innerContent};
            }

            String source = innerContent.substring(0, lastColonIndex);
            String remainder = innerContent.substring(lastColonIndex + 1);

            int secondLastColonIndex = remainder.lastIndexOf(":");
            if (secondLastColonIndex == -1) {
                return new String[]{source, remainder};
            } else {
                String format = remainder.substring(0, secondLastColonIndex);
                return new String[]{source, format};
            }
        }
    }

    private String[] parseKnownSource(String innerContent, String source) {
        String remainder = innerContent.substring((source + ":").length());
        int lastColonIndex = remainder.lastIndexOf(":");
        if (lastColonIndex == -1) {
            return new String[]{source, remainder};
        } else {
            String possibleAttributeName = remainder.substring(lastColonIndex + 1);
            if (possibleAttributeName.matches(ATTRIBUTE_NAME_PATTERN) && possibleAttributeName.length() <= 50) {
                String format = remainder.substring(0, lastColonIndex);
                return new String[]{source, format};
            } else {
                return new String[]{source, remainder};
            }
        }
    }

    @Override
    public boolean supports(String template) {
        return template != null &&
                template.startsWith("${datetime:") &&
                template.contains(":") &&
                template.endsWith("}");
    }

    @Override
    public String getTemplatePattern() {
        return "datetime:[^}]+";
    }

    @Override
    public String getName() {
        return NAME;
    }
}
