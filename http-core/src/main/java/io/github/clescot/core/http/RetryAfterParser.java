package io.github.clescot.core.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;

import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * Parses a {@code Retry-After} / {@code X-Retry-After} header value into the number of seconds
 * to wait.
 *
 * <p>The value is either a number of seconds or an HTTP date. Two date formats are accepted, RFC
 * 7231 and RFC 1123, so that the separators the header is commonly written with are both
 * understood. The values the sink and the SSE source act on must be interpreted identically, so
 * the parsing lives here rather than in each connector.
 */
public final class RetryAfterParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetryAfterParser.class);
    private static final String UTC = "UTC";
    private static final Pattern IS_INTEGER = Pattern.compile("\\d+");
    private static final DateTimeFormatter RFC_7231_FORMATTER = DateTimeFormatter.ofPattern(HttpResponse.RFC_7231_PATTERN);
    private static final DateTimeFormatter RFC_1123_FORMATTER = DateTimeFormatter.RFC_1123_DATE_TIME;

    private RetryAfterParser() {
    }

    /**
     * @param value the value of the Retry-After header
     * @return the number of seconds to wait, 0 when the value cannot be understood
     */
    public static long secondsToWait(String value) {
        return secondsToWait(value, Clock.systemUTC());
    }

    /**
     * @param value the value of the Retry-After header
     * @param clock the clock deciding how far a date-form value is in the future
     * @return the number of seconds to wait, 0 when the value cannot be understood
     */
    public static long secondsToWait(String value, Clock clock) {
        if (value == null) {
            return 0L;
        }
        if (IS_INTEGER.matcher(value).matches()) {
            return Integer.parseInt(value);
        }
        Instant until;
        try {
            until = LocalDateTime.parse(value, RFC_7231_FORMATTER).atZone(ZoneId.of(UTC)).toInstant();
        } catch (DateTimeParseException dtp) {
            LOGGER.warn("Cannot parse Retry-After / X-Retry-After header value '{}' as a date with RFC7231 format, falling back to RFC1123 format", value);
            try {
                until = ZonedDateTime.parse(value, RFC_1123_FORMATTER).toInstant();
            } catch (DateTimeParseException dtp2) {
                LOGGER.error("Cannot parse Retry-After / X-Retry-After header value '{}' as a date with RFC1123 format either, falling back retry", value);
                return 0L;
            }
        }
        return clock.instant().until(until, SECONDS);
    }
}
