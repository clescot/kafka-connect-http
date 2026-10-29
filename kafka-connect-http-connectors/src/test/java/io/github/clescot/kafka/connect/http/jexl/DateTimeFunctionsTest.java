package io.github.clescot.kafka.connect.http.jexl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DateTimeFunctionsTest {

  private static final long TIMESTAMP_IN_MS = 1699228800000L;
  private static final String DATE_TIME = "2023-11-06T00:00:00.000Z";

  @Nested
  class Format {

    @Test
    void test_format_an_iso_8601_string() {
      assertThat(DateTimeFunctions.format(DATE_TIME, "yyyy-MM-dd")).isEqualTo("2023-11-06");
    }

    @Test
    void test_format_is_in_utc() {
      assertThat(DateTimeFunctions.format(DATE_TIME, "yyyy-MM-dd HH:mm:ss.SSSXXX"))
          .isEqualTo("2023-11-06 00:00:00.000Z");
    }

    @Test
    void test_format_supports_the_pattern_letters_of_each_iso_8601_part() {
      assertThat(DateTimeFunctions.format(DATE_TIME, "yyyy")).isEqualTo("2023");
      assertThat(DateTimeFunctions.format(DATE_TIME, "MM")).isEqualTo("11");
      assertThat(DateTimeFunctions.format(DATE_TIME, "dd")).isEqualTo("06");
      assertThat(DateTimeFunctions.format(DATE_TIME, "HH")).isEqualTo("00");
      assertThat(DateTimeFunctions.format(DATE_TIME, "mm")).isEqualTo("00");
      assertThat(DateTimeFunctions.format(DATE_TIME, "ss")).isEqualTo("00");
      assertThat(DateTimeFunctions.format(DATE_TIME, "SSS")).isEqualTo("000");
      assertThat(DateTimeFunctions.format(DATE_TIME, "XXX")).isEqualTo("Z");
    }

    @Test
    void test_textual_fields_do_not_depend_on_the_host_locale() {
      assertThat(DateTimeFunctions.format(DATE_TIME, "EEEE d MMMM yyyy"))
          .isEqualTo("Monday 6 November 2023");
    }

    @Test
    void test_format_a_value_that_is_not_a_date_and_time() {
      assertThatThrownBy(() -> DateTimeFunctions.format("not a date", "yyyy"))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void test_format_with_a_null_pattern() {
      assertThatThrownBy(() -> DateTimeFunctions.format(DATE_TIME, null))
          .isInstanceOf(NullPointerException.class);
    }
  }

  @Nested
  class FromEpochMillis {

    @Test
    void test_from_epoch_millis() {
      assertThat(DateTimeFunctions.fromEpochMillis(TIMESTAMP_IN_MS, "yyyy-MM-dd HH:mm:ss"))
          .isEqualTo("2023-11-06 00:00:00");
    }
  }

  @Nested
  class Now {

    @Test
    void test_now_is_iso_8601_in_utc() {
      long before = System.currentTimeMillis();
      Instant now = Instant.parse(DateTimeFunctions.now());
      long after = System.currentTimeMillis();

      assertThat(now.toEpochMilli()).isBetween(before, after);
    }

    @Test
    void test_now_with_a_pattern() {
      int yearBefore = Instant.now().atZone(DateTimeFunctions.ZONE).getYear();
      String formatted = DateTimeFunctions.now("yyyy");
      int yearAfter = Instant.now().atZone(DateTimeFunctions.ZONE).getYear();

      assertThat(formatted).isIn(String.valueOf(yearBefore), String.valueOf(yearAfter));
    }

    @Test
    void test_now_with_a_null_pattern() {
      assertThatThrownBy(() -> DateTimeFunctions.now(null))
          .isInstanceOf(NullPointerException.class);
    }
  }
}
