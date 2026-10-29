package io.github.clescot.kafka.connect.http.sink;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkTask.FROM_STRING_PART_TO_SINK_RECORD_FUNCTION;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.clescot.kafka.connect.http.MessageSplitter;
import io.github.clescot.kafka.connect.http.jexl.JexlEngineFactory;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class MessageSplitterTest {
  @Nested
  class Constructor {

    @Test
    void test_with_null_params() {
      Assertions.assertThrows(
          NullPointerException.class,
          () ->
              new MessageSplitter(
                  FROM_STRING_PART_TO_SINK_RECORD_FUNCTION, null, null, null, null, 0));
    }

    @Test
    void test_with_null_id() {
      JexlEngine jexlEngine = JexlEngineFactory.build();
      Assertions.assertThrows(
          NullPointerException.class,
          () ->
              new MessageSplitter(
                  FROM_STRING_PART_TO_SINK_RECORD_FUNCTION, null, jexlEngine, "", "\\n", 0));
    }

    @Test
    void test_with_null_matching_expression() {
      JexlEngine jexlEngine = JexlEngineFactory.build();
      Assertions.assertThrows(
          NullPointerException.class,
          () ->
              new MessageSplitter(
                  FROM_STRING_PART_TO_SINK_RECORD_FUNCTION, "config1", jexlEngine, null, "\\n", 0));
    }

    @Test
    void test_with_null_jexl_engine() {
      Assertions.assertThrows(
          NullPointerException.class,
          () ->
              new MessageSplitter(
                  FROM_STRING_PART_TO_SINK_RECORD_FUNCTION,
                  "myid",
                  null,
                  "message.splitter.myid.matcher",
                  "\\n",
                  0));
    }

    @Test
    void test_with_null_split_pattern() {
      JexlEngine jexlEngine = JexlEngineFactory.build();
      Assertions.assertThrows(
          NullPointerException.class,
          () ->
              new MessageSplitter(
                  FROM_STRING_PART_TO_SINK_RECORD_FUNCTION,
                  "myid",
                  jexlEngine,
                  "message.splitter.myid.matcher",
                  null,
                  0));
    }

    @Test
    void test_with_no_split_limit() {
      JexlEngine jexlEngine = JexlEngineFactory.build();
      Assertions.assertDoesNotThrow(
          () ->
              new MessageSplitter(
                  FROM_STRING_PART_TO_SINK_RECORD_FUNCTION,
                  "myid",
                  jexlEngine,
                  "message.splitter.myid.matcher",
                  "\\n",
                  0));
    }
  }

  @Nested
  class Matches {

    private static final long TIMESTAMP_IN_MS = 1699228800000L;

    private SinkRecord record() {
      return new SinkRecord(
          "myTopic",
          0,
          Schema.STRING_SCHEMA,
          "key",
          Schema.STRING_SCHEMA,
          "a\nb",
          0,
          TIMESTAMP_IN_MS,
          TimestampType.CREATE_TIME);
    }

    @Test
    void test_matcher_can_use_the_record_date_time() {
      MessageSplitter<SinkRecord> splitter =
          new MessageSplitter<>(
              FROM_STRING_PART_TO_SINK_RECORD_FUNCTION,
              "myid",
              JexlEngineFactory.build(),
              "dateTime=='2023-11-06T00:00:00.000Z'",
              "\\n",
              0);

      assertThat(splitter.matches(record())).isTrue();
    }

    @Test
    void test_matcher_can_use_a_custom_date_time_pattern() {
      MessageSplitter<SinkRecord> splitter =
          new MessageSplitter<>(
              FROM_STRING_PART_TO_SINK_RECORD_FUNCTION,
              "myid",
              JexlEngineFactory.build(),
              "datetime:fromEpochMillis(timestampInMs,'yyyy-MM-dd')=='2023-11-06'",
              "\\n",
              0);

      assertThat(splitter.matches(record())).isTrue();
    }
  }
}
