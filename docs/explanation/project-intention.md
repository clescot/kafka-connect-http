# Project intention

## The problem

[Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) copies data
between Kafka and external systems. By design it is **one-way**:

- **Sink** connectors read from Kafka and write to an external target.
- **Source** connectors read from an external source and write to Kafka.

HTTP is **two-way**: a request produces a response. Kafka Connect does not model that
interaction, so it has no native way to call an HTTP server and keep the answer.

## The goal

This project lets a Kafka Connect cluster interact with HTTP servers: execute requests
described by Kafka messages, and, optionally, store the HTTP responses as Kafka messages.

## How it differs from other connectors

Other Kafka Connect connectors also talk to HTTP servers, but tend to have one of these
limitations:

- some ignore the HTTP responses entirely;
- some return responses but force a single configuration that mixes the high-level Kafka
  Connect connection with a low-level configuration to publish responses — awkward when the
  cluster connection relies on third-party libraries or a custom classloader;
- some are proprietary.

This project separates the two concerns. The high-level path lets Kafka Connect handle the
Kafka connection, and the choice of what to do with responses is explicit and per-connector.

## How the two-way problem is solved

Because Kafka Connect is one-way, the publish step is split into modes with different
trade-offs. Each mode decides what happens to an HTTP response:

- ignore it,
- publish it with the connector's own producer,
- publish it through an in-memory queue consumed by a source connector,
- or route bad messages to a dead-letter queue.

The modes are described in [publish modes](publish-modes.md), and how to configure them in
[choose a publish mode](../how-to/choose-a-publish-mode.md).

## Where to go next

- How a message becomes an HTTP call: [architecture](architecture.md).
- Async execution, rate limiting, and retries: [request handling](request-handling.md).
- Spreading the load: [scalability](scalability.md).
