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

## Choosing

| Mode | Responses | Duplicated config | Locality |
| --- | --- | --- | --- |
| `NONE` | discarded | no | no |
| `PRODUCER` | to Kafka | yes | no |

Start with `NONE`. Choose `PRODUCER` when you need the responses.
