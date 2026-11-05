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

  /**
   * Tells whether Kafka Connect's offset storage is reachable at all. A task that Connect never
   * initialised (an embedded task, a unit test) has none, and "nothing was committed" is then not a
   * statement about the configuration: a {@code fail} resume policy must not treat it as a gap to
   * refuse.
   *
   * @return true when committed offsets can be read back
   */
  default boolean hasOffsetStorage() {
    return true;
  }

  /**
   * Lookup for a task with no offset storage: no committed offset can ever be found, so the
   * configuration's Resume Policy has nothing to work from.
   */
  SseResumeLookup NONE =
      new SseResumeLookup() {
        @Override
        public Optional<String> lastCommittedEventId(String configurationId) {
          return Optional.empty();
        }

        @Override
        public boolean hasOffsetStorage() {
          return false;
        }
      };
}
