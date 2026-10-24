# Choose a publish mode

The HTTP Sink connector decides what to do with HTTP responses based on a single setting,
`publish.mode`. This guide helps you pick the right value and shows the configuration each
one needs.

For the reasoning behind these modes, see [publish modes](../explanation/publish-modes.md).

## The options at a glance

| `publish.mode` | Responses are… | Needs a source connector? | Co-location needed? |
| --- | --- | --- | --- |
| `NONE` (default) | discarded | no | no |
| `PRODUCER` | published to Kafka by the sink's own producer | no | no |
| `IN_MEMORY_QUEUE` | published to Kafka via an in-memory queue + source connector | yes | **yes** |
| `DLQ` | handed to the dead-letter-queue reporter | no | no |

## `NONE` — fire and forget

Use it when you only care that the request was sent. It is the simplest configuration,
mixes nothing into your topics, and has no locality constraint.

```json
{ "publish.mode": "NONE" }
```

**Trade-off:** you lose the HTTP responses entirely.

## `PRODUCER` — publish responses with a low-level producer

The connector uses its own Kafka producer to write each exchange (or just the response) to a
success topic and an error topic. There is no locality constraint, but you must supply the
producer connection details yourself.

```json
{
  "publish.mode": "PRODUCER",
  "producer.bootstrap.servers": "kafka:9092",
  "producer.schema.registry.url": "https://my-schema-registry:8081",
  "producer.success.topic": "http-success",
  "producer.error.topic": "http-error",
  "producer.content": "exchange"
}
```

**Trade-off:** you duplicate broker/registry connection configuration at the connector level.
That duplication is awkward when the cluster connection relies on third-party libraries or a
custom classloader. Choose `IN_MEMORY_QUEUE` instead if you want to reuse the Connect
worker's own configuration.

## `IN_MEMORY_QUEUE` — reuse the Connect worker

The sink connector writes each exchange to an in-memory queue. A
`HttpInMemoryQueueSourceConnector` running in the same worker reads the queue and publishes
to Kafka. This keeps both connectors on high-level Kafka Connect configuration.

Sink connector:

```json
{
  "publish.mode": "IN_MEMORY_QUEUE",
  "queue.name": "default"
}
```

Source connector (same worker, same `queue.name`, same `tasks.max`):

```json
{
  "connector.class": "io.github.clescot.kafka.connect.http.source.queue.HttpInMemoryQueueSourceConnector",
  "success.topic": "http-success",
  "error.topic": "http-error",
  "queue.name": "default"
}
```

Because the queue lives in memory, the sinks and sources must be co-located:

- configure the **same `tasks.max`** for both connectors;
- give the topics used by the sink and the source the **same number of partitions**, so a
  record with a given key lands in the same partition on both sides;
- deploy no more workers than the partition count.

**Trade-off:** co-location constrains how you distribute the connectors. It does not remove
Kafka Connect's distributed nature: the HTTP requests themselves can still be spread across
workers, and only the request/response correlation stays local.

If you set `publish.mode` to `IN_MEMORY_QUEUE` without a consumer for the queue, the sink
connector fails on the first message rather than growing the queue without bound. You can
tune that wait with `wait.time.registration.queue.consumer.in.ms` (default 60000).

## `DLQ` — route bad messages to a dead-letter queue

Use it when a message cannot be turned into a valid HTTP request, and you want the built-in
errant-record reporter to send it to a dead-letter topic instead of failing the task.

```json
{ "publish.mode": "DLQ" }
```

## Related settings

- `queue.name` — the in-memory queue to use (default `default`). Only meaningful with
  `IN_MEMORY_QUEUE`.
- `wait.time.registration.queue.consumer.in.ms`, `poll.delay.registration.queue.consumer.in.ms`,
  `poll.interval.registration.queue.consumer.in.ms` — tune how the sink waits for, and
  polls, the source connector's queue registration.
- `producer.*` — every producer setting, used only with `PRODUCER`.

Full defaults are in the [HTTP Sink reference](../reference/http-sink-connector.md).
