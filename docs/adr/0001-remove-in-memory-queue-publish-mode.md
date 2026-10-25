# Remove the in-memory queue publish mode

The HTTP sink connector had a third publish mode, `IN_MEMORY_QUEUE`. It passed each HTTP exchange to a JVM-local
unbounded queue, and a colocated HTTP source connector drained that queue. The mode only works when the sink and the
source connector run in the same worker process and the same classloader. That contradicts the distributed execution
model of Kafka Connect, and the queue buffers without limit. The sink now discards each exchange (`NONE`) or publishes
it to Kafka topics (`PRODUCER`).

## Consequences

- `publish.mode=IN_MEMORY_QUEUE` is no longer a valid value, so an existing configuration fails at startup. Migrate to
  `PRODUCER`, using the table in [choose a publish mode](../how-to/choose-a-publish-mode.md).
- The `queue.name` setting and the three `*.registration.queue.consumer.in.ms` settings are removed.
- The `HttpInMemoryQueueSourceConnector` and its `KafkaRecord` transport type are removed. The HTTP source connector
  reference page is removed with them.
- `QueueFactory` stays, because the cron and the SSE source connectors use it as a private buffer for one task.

## Considered options

- **Keep the mode behind a deprecation warning.** Rejected. The mode stays broken on a distributed cluster, and no
  documentation removes the colocation requirement.
- **Build a Kafka-topic-backed replacement that copies the queue behavior.** Rejected. It would repeat `PRODUCER` with
  more configuration, and a topic cannot give the single-consumer behavior that the queue relied on.
