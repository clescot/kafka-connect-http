package io.github.clescot.kafka.connect.http.jexl;

import com.google.common.base.Preconditions;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;

/**
 * Functions exposed to JEXL expressions under the {@code datetime} namespace. They format a date
 * and time with any {@link DateTimeFormatter} pattern, in UTC and with the English locale, so that
 * the output does not depend on the host.
 *
 * <p>Example: {@code datetime:format(dateTime,'yyyy-MM-dd')}.
 */
public final class DateTimeFunctions {

  /** Name of the JEXL namespace exposing these functions. */
  public static final String NAMESPACE = "datetime";

  /** Zone used by all these functions, so that a formatted value does not depend on the host. */
  public static final ZoneId ZONE = ZoneOffset.UTC;

  /** Locale used by all these functions, so that textual fields do not depend on the host. */
  public static final Locale LOCALE = Locale.ENGLISH;

  private static final String ISO_8601_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSX";
  private static final DateTimeFormatter ISO_8601 = formatter(ISO_8601_PATTERN);

  private DateTimeFunctions() {}

  /** Formats an instant as ISO-8601 in UTC. */
  static String iso(Instant instant) {
    return ISO_8601.format(instant);
  }

  /** Returns the current date and time as ISO-8601 in UTC. */
  public static String now() {
    return iso(Instant.now());
  }

  /** Returns the current date and time formatted with the given pattern, in UTC. */
  public static String now(String pattern) {
    return formatter(pattern).format(Instant.now());
  }

  /**
   * Formats an ISO-8601 date and time with the given {@link DateTimeFormatter} pattern, in UTC.
   *
   * @param isoDateTime an ISO-8601 date and time, e.g. the {@code dateTime} JEXL variable
   * @param pattern a {@link DateTimeFormatter} pattern, e.g. {@code yyyy-MM-dd HH:mm:ss}
   * @return the formatted value
   */
  public static String format(String isoDateTime, String pattern) {
    Preconditions.checkNotNull(isoDateTime, "isoDateTime is required");
    return formatter(pattern).format(parse(isoDateTime));
  }

  /**
   * Formats an epoch-millis value with the given pattern, in UTC.
   *
   * @param epochMillis an epoch-millis value, e.g. the {@code timestampInMs} JEXL variable
   * @param pattern a {@link DateTimeFormatter} pattern
   * @return the formatted value
   */
  public static String fromEpochMillis(long epochMillis, String pattern) {
    return formatter(pattern).format(Instant.ofEpochMilli(epochMillis));
  }

  private static DateTimeFormatter formatter(String pattern) {
    Preconditions.checkNotNull(pattern, "pattern is required");
    return DateTimeFormatter.ofPattern(pattern, LOCALE).withZone(ZONE);
  }

  private static Instant parse(String isoDateTime) {
    try {
      return Instant.parse(isoDateTime);
    } catch (DateTimeParseException ignored) {
      // not an ISO instant, try the other ISO-8601 shapes
    }
    try {
      return OffsetDateTime.parse(isoDateTime).toInstant();
    } catch (DateTimeParseException ignored) {
      // no offset
    }
    try {
      return ZonedDateTime.parse(isoDateTime).toInstant();
    } catch (DateTimeParseException ignored) {
      // no zone
    }
    throw new IllegalArgumentException(
        "cannot read '" + isoDateTime + "' as an ISO-8601 date and time");
  }
}
