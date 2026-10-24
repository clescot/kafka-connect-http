# HTTP Source connector reference

Reference for
`io.github.clescot.kafka.connect.http.source.queue.HttpInMemoryQueueSourceConnector`.

This connector reads `HttpExchange` (or `HttpResponse`) values from an in-memory queue and
publishes them to Kafka. It is only useful with `publish.mode: IN_MEMORY_QUEUE` on the HTTP
Sink connector. It has no other purpose: with `NONE` or `PRODUCER`, no source connector is
needed.

## Required settings

| Setting | Description |
| --- | --- |
| `connector.class` | `io.github.clescot.kafka.connect.http.source.queue.HttpInMemoryQueueSourceConnector` |
| `success.topic` | Topic that receives successful exchanges. |
| `error.topic` | Topic that receives failed exchanges. |

## Optional settings

| Setting | Default | Description |
| --- | --- | --- |
| `tasks.max` | `1` | Must match the sink connector's `tasks.max`. |
| `queue.name` | `default` | Queue to read. Must match the sink connector's `queue.name`. |
| `content` | `exchange` | `exchange` (full `HttpExchange`) or `response` (only `HttpResponse`). |

## Coupling constraints

Because the queue is in memory, the source connector must:

- run in the **same Kafka Connect instance** as the sink connector;
- use the **same `queue.name`** and the **same `tasks.max`** as the sink connector.

Additionally, give the sink and source topics the **same number of partitions** and keep the
default partitioner, so a record with a given key produces an exchange stored in the same
partition on the source side. See
[choose a publish mode](../how-to/choose-a-publish-mode.md).

> Templating (`exchange.template`) is applied where the exchange is **produced** — on the
> HTTP Sink connector — not here. Setting it on this connector has no effect.

## Example

`source.json`:

```json
{
  "name": "my-http-source-connector",
  "config": {
    "connector.class": "io.github.clescot.kafka.connect.http.source.queue.HttpInMemoryQueueSourceConnector",
    "tasks.max": "1",
    "success.topic": "http-success",
    "error.topic": "http-error",
    "queue.name": "default"
  }
}
```

```bash
curl -X PUT -H "Content-Type: application/json" \
  --data @source.json \
  http://my-kafka-connect-cluster:8083/connectors/my-http-source-connector/config
```
