package io.github.clescot.kafka.connect.http.jexl;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.apache.commons.jexl3.JexlContext;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SinkRecordJexlContextTest {

  private static final long TIMESTAMP_IN_MS = 1699228800000L;

  private static SinkRecord recordWithTimestamp(Long timestamp) {
    return new SinkRecord(
        "myTopic",
        0,
        Schema.STRING_SCHEMA,
        "key",
        Schema.STRING_SCHEMA,
        "value",
        0,
        timestamp,
        TimestampType.CREATE_TIME);
  }

  @Nested
  class EventTime {

    @Test
    void test_sink_record_is_exposed() {
      SinkRecord sinkRecord = recordWithTimestamp(TIMESTAMP_IN_MS);

      JexlContext context = SinkRecordJexlContext.contextFor(sinkRecord);

      assertThat(context.get(SinkRecordJexlContext.SINK_RECORD)).isSameAs(sinkRecord);
    }

    @Test
    void test_timestamp_is_the_event_time_in_epoch_seconds() {
      JexlContext context = SinkRecordJexlContext.contextFor(recordWithTimestamp(TIMESTAMP_IN_MS));

      assertThat(context.get(SinkRecordJexlContext.TIMESTAMP)).isEqualTo(1699228800L);
    }

    @Test
    void test_timestamp_in_ms_is_the_event_time_in_epoch_millis() {
      JexlContext context = SinkRecordJexlContext.contextFor(recordWithTimestamp(TIMESTAMP_IN_MS));

      assertThat(context.get(SinkRecordJexlContext.TIMESTAMP_IN_MS)).isEqualTo(TIMESTAMP_IN_MS);
    }

    @Test
    void test_date_time_is_the_event_time_in_iso_8601_utc() {
      JexlContext context = SinkRecordJexlContext.contextFor(recordWithTimestamp(TIMESTAMP_IN_MS));

      assertThat(context.get(SinkRecordJexlContext.DATE_TIME))
          .isEqualTo("2023-11-06T00:00:00.000Z");
    }

    @Test
    void test_missing_event_time_falls_back_to_now() {
      long before = System.currentTimeMillis();
      JexlContext context = SinkRecordJexlContext.contextFor(recordWithTimestamp(null));
      long after = System.currentTimeMillis();

      long timestampInMs = (Long) context.get(SinkRecordJexlContext.TIMESTAMP_IN_MS);
      assertThat(timestampInMs).isBetween(before, after);
      assertThat(context.get(SinkRecordJexlContext.TIMESTAMP))
          .isEqualTo(Instant.ofEpochMilli(timestampInMs).getEpochSecond());
      assertThat(Instant.parse((String) context.get(SinkRecordJexlContext.DATE_TIME)))
          .isEqualTo(Instant.ofEpochMilli(timestampInMs));
    }

    @Test
    void test_negative_event_time_falls_back_to_now() {
      long before = System.currentTimeMillis();
      JexlContext context = SinkRecordJexlContext.contextFor(recordWithTimestamp(-1L));
      long after = System.currentTimeMillis();

      assertThat((Long) context.get(SinkRecordJexlContext.TIMESTAMP_IN_MS))
          .isBetween(before, after);
    }
  }
}
