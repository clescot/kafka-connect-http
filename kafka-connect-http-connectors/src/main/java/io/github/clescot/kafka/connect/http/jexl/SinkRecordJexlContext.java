package io.github.clescot.kafka.connect.http.jexl;

import com.google.common.base.Preconditions;
import java.time.Instant;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.MapContext;
import org.apache.kafka.connect.connector.ConnectRecord;

/**
 * Builds the JEXL context shared by the sink-side expressions (request mappers and message
 * splitters).
 *
 * <p>It exposes the record as {@code sinkRecord}, plus the record event time as {@code timestamp}
 * (epoch seconds), {@code timestampInMs} (epoch millis) and {@code dateTime} (ISO-8601 in UTC).
 * When a record carries no event time, the current time is used instead.
 */
public final class SinkRecordJexlContext {

  public static final String SINK_RECORD = "sinkRecord";
  public static final String TIMESTAMP = "timestamp";
  public static final String TIMESTAMP_IN_MS = "timestampInMs";
  public static final String DATE_TIME = "dateTime";

  private SinkRecordJexlContext() {}

  /** Builds the JEXL context for the given record. */
  public static JexlContext contextFor(ConnectRecord<?> record) {
    Preconditions.checkNotNull(record, "record is required");
    Instant eventTime = eventTimeOf(record);
    MapContext context = new MapContext();
    context.set(SINK_RECORD, record);
    context.set(TIMESTAMP, eventTime.getEpochSecond());
    context.set(TIMESTAMP_IN_MS, eventTime.toEpochMilli());
    context.set(DATE_TIME, DateTimeFunctions.iso(eventTime));
    return context;
  }

  private static Instant eventTimeOf(ConnectRecord<?> record) {
    Long timestamp = record.timestamp();
    return timestamp != null && timestamp >= 0 ? Instant.ofEpochMilli(timestamp) : Instant.now();
  }
}
