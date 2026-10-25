# Choose a publish mode

The HTTP Sink connector decides what to do with HTTP responses based on a single setting,
`publish.mode`. This guide helps you pick the right value and shows the configuration each
one needs.

For the reasoning behind these modes, see [publish modes](../explanation/publish-modes.md).

## The options at a glance

| `publish.mode` | Responses are… | Duplicated config |
| --- | --- | --- |
| `NONE` (default) | discarded | no |
| `PRODUCER` | published to Kafka by the sink's own producer | yes |

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

**Trade-off:** you duplicate broker and registry connection configuration at the connector
level. That duplication is awkward when the cluster connection relies on third-party libraries
or a custom classloader.

## Migrating from `IN_MEMORY_QUEUE`

The `IN_MEMORY_QUEUE` publish mode is removed. It worked only when an HTTP sink and an HTTP
source connector ran in the same worker process. That requirement conflicts with the
distributed execution model of Kafka Connect. See
[ADR 0001](../adr/0001-remove-in-memory-queue-publish-mode.md).

Replace these settings in your HTTP sink connector configuration:

| Removed setting | Replacement |
| --- | --- |
| `publish.mode: IN_MEMORY_QUEUE` | `publish.mode: PRODUCER` |
| `queue.name` | `producer.success.topic` and `producer.error.topic` |
| `wait.time.registration.queue.consumer.in.ms` | not needed |
| `poll.delay.registration.queue.consumer.in.ms` | not needed |
| `poll.interval.registration.queue.consumer.in.ms` | not needed |

Remove the `HttpInMemoryQueueSourceConnector` instance as well. `PRODUCER` also needs
`producer.bootstrap.servers`.

## Related settings

- `producer.*` — every producer setting, used only with `PRODUCER`.

Full defaults are in the [HTTP Sink reference](../reference/http-sink-connector.md).
