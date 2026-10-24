# Publish modes

Kafka Connect connectors are one-way. HTTP is two-way. The publish mode is where this project
reconciles the two: it decides what happens to the response half of an exchange.

For the configuration of each mode, see
[choose a publish mode](../how-to/choose-a-publish-mode.md).

## `NONE` — accept the loss

The simplest answer: send the request and ignore the response. It needs one connector, has no
locality constraint, and keeps responses out of the data topics. The cost is that the HTTP
response is lost.

This is the default.

## `PRODUCER` — publish it yourself

The connector uses its own low-level Kafka producer to write responses to a success topic and
an error topic. Nothing is lost, and there is still no locality constraint. The cost is
configuration duplication: the producer needs its own broker and schema-registry connection
details, which you must maintain alongside the worker's configuration.

That duplication is painful when the cluster connection depends on third-party libraries or a
custom classloader, because those are hard to reproduce at the connector level.

## `IN_MEMORY_QUEUE` — reuse the worker

The connector writes each exchange to a shared **in-memory queue**, and a source connector in
the same worker reads it and publishes to Kafka. Both connectors stay on high-level Kafka
Connect configuration, so the awkward duplication of `PRODUCER` disappears.

The cost is locality:

- the sink and source connectors must run in the **same worker** (the queue is in memory);
- they must share `queue.name` and `tasks.max`;
- the topics on both sides should use the **same partition count** so a keyed record maps to
  the same partition.

Locality is only about correlating a request with its response — the HTTP calls themselves can
still be distributed across workers.

If no consumer is registered for the queue, the sink connector **fails on the first message**
rather than growing the queue until the JVM runs out of memory.

## `DLQ` — route bad messages

When a message cannot be turned into a valid request, the built-in errant-record reporter
sends it to a dead-letter topic instead of failing the task. Use it to keep a task alive while
isolating unprocessable input.

## Choosing

| Mode | Responses | Extra connector | Duplicated config | Locality |
| --- | --- | --- | --- | --- |
| `NONE` | discarded | no | no | no |
| `PRODUCER` | to Kafka | no | yes | no |
| `IN_MEMORY_QUEUE` | to Kafka | yes | no | yes |
| `DLQ` | to a dead-letter topic | no | no | no |

Start with `NONE`. Add `IN_MEMORY_QUEUE` when you need responses and want to keep
configuration simple and co-location is acceptable. Choose `PRODUCER` when you need responses
but cannot co-locate the connectors.
