package io.github.clescot.kafka.connect.sse.client.okhttp;

import java.util.Optional;

/**
 * Looks up the resume cursor of an SSE configuration: the highest event id Kafka Connect committed
 * for it, so a restarted task can ask the SSE server to resume from there.
 */
@FunctionalInterface
public interface SseResumeLookup {

  /**
   * Returns the highest event id Kafka Connect committed for this SSE configuration, or empty when
   * nothing was committed for it yet.
   *
   * @param configurationId configuration id
   * @return the committed event id, or empty
   */
  Optional<String> lastCommittedEventId(String configurationId);

  /** Lookup that never finds a committed offset: the server's own resume policy applies. */
  SseResumeLookup NONE = configurationId -> Optional.empty();
}
