package io.github.clescot.kafka.connect.sse.client.okhttp;

import static io.github.clescot.core.http.VersionUtils.VERSION;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.core.http.HttpRequest;
import io.github.clescot.core.sse.SseEvent;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SseSourceTask extends SourceTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(SseSourceTask.class);
  static final String PARTITION_CONFIGURATION_ID = "configurationId";
  static final String OFFSET_EVENT_ID = "eventId";
  private SseTask sseTask;

  @Override
  public String version() {
    return VERSION;
  }

  static Map<String, String> sourcePartition(String configurationId) {
    return Map.of(PARTITION_CONFIGURATION_ID, configurationId);
  }

  /**
   * Builds the lookup of the event ids Kafka Connect committed. The Kafka Connect transport stays
   * at this boundary: a task that was never initialised by Connect (unit tests) has no offset
   * storage, so nothing was committed and the server's own resume policy applies.
   */
  SseResumeLookup resumeLookup() {
    OffsetStorageReader reader = context == null ? null : context.offsetStorageReader();
    if (reader == null) {
      return SseResumeLookup.NONE;
    }
    return configurationId -> {
      Map<String, Object> committed = reader.offset(sourcePartition(configurationId));
      if (committed == null) {
        return Optional.empty();
      }
      Object eventId = committed.get(OFFSET_EVENT_ID);
      return eventId == null ? Optional.empty() : Optional.of(eventId.toString());
    };
  }

  @Override
  public void start(Map<String, String> settings) {
    Preconditions.checkNotNull(settings, "settings must not be null or empty.");
    this.sseTask = new SseTask(settings, resumeLookup());
    this.sseTask.connect();
    this.sseTask.start();
  }

  @Override
  public List<SourceRecord> poll() {
    Map<String, Queue<SseEvent>> queues = this.sseTask.getQueues();
    Map<String, SseConfiguration> configurations = this.sseTask.getConfigurations();
    List<SourceRecord> records = Lists.newArrayList();
    for (Map.Entry<String, Queue<SseEvent>> queue : queues.entrySet()) {
      records.addAll(poll(configurations.get(queue.getKey()), queue.getValue()));
    }

    return records;
  }

  private List<SourceRecord> poll(SseConfiguration sseConfiguration, Queue<SseEvent> queue) {
    Preconditions.checkNotNull(queue, "queue must not be null.");
    List<SourceRecord> records = Lists.newArrayList();

    // Create a basic HTTP request for template processing context
    // This represents the SSE connection request
    HttpRequest httpRequest = new HttpRequest(sseConfiguration.getUri().toString());

    // the partition is the SSE connection, not the event type: it is the unit the committed
    // offsets are keyed by, and the unit the resume cursor is read back for
    Map<String, String> partition = sourcePartition(sseConfiguration.getConfigurationId());
    while (queue.peek() != null) {
      SseEvent sseEvent = queue.poll();
      LOGGER.debug(
          "Polled from queue: {} event: {} ", sseEvent, sseConfiguration.getConfigurationId());

      // Apply template processing to the SSE event
      SseEvent processedEvent = sseConfiguration.processEventWithTemplate(sseEvent, httpRequest);
      SseEvent eventToUse = (processedEvent != null) ? processedEvent : sseEvent;

      // The cursor is the id the SSE server sent, never the id a template may have rewritten:
      // Last-Event-ID is only meaningful inside the server's own id space.
      Map<String, String> sourceOffset =
          sseEvent.getId() != null ? Map.of(OFFSET_EVENT_ID, sseEvent.getId()) : Maps.newHashMap();
      SourceRecord sourceRecord =
          new SourceRecord(
              partition,
              sourceOffset,
              sseConfiguration.getTopic(),
              Schema.STRING_SCHEMA,
              eventToUse.getType(),
              SseEvent.SCHEMA,
              eventToUse.toJson());
      records.add(sourceRecord);
    }
    return records;
  }

  @Override
  public void stop() {
    if (this.sseTask != null) {
      this.sseTask.stop();
    }
  }

  public Optional<Queue<SseEvent>> getQueue(String configurationId) {
    return this.sseTask.getQueue(configurationId);
  }

  public Map<String, Queue<SseEvent>> getQueues() {
    return this.sseTask.getQueues();
  }

  public boolean isConnected(String configurationId) {
    Preconditions.checkNotNull(
        this.sseTask, "sseTask must not be null. call 'start()' method first.");
    return this.sseTask.isConnected(configurationId);
  }

  public Map<String, SseConfiguration> getConfigurations() {
    return this.sseTask.getConfigurations();
  }
}
