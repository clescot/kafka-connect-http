# kafka-connect-http documentation

This documentation is organised with the [Diátaxis](https://diataxis.fr/) framework.
Each page serves one purpose. Start from the section that matches your goal.

## Learn (tutorials)

Step-by-step lessons that take you from nothing to a working result.

- [Your first HTTP exchange](tutorials/first-http-exchange.md) — deploy a sink connector
  and see a request travel from Kafka to an HTTP server and back.

## Do a task (how-to guides)

Recipes that solve a specific, real-world problem. They assume you already know the basics.

- [Install the connectors](how-to/install.md) — get the plugin into a Kafka Connect image
  and verify it is loaded.
- [Quick start configurations](how-to/quick-start.md) — copy-paste connector configurations.
- [Choose a publish mode](how-to/choose-a-publish-mode.md) — decide what happens to HTTP
  responses.
- [Configure authentication](how-to/configure-authentication.md) — Basic, Digest, OAuth2,
  SSL, and proxy authentication.
- [Customize exchange templates](how-to/customize-exchange-templates.md) — rewrite the
  published exchange before it reaches Kafka.
- [Troubleshoot](how-to/troubleshoot.md) — diagnose the most common problems.

## Look something up (reference)

Precise, complete descriptions of the machinery. These pages are derived from the connector
configuration definitions so they stay accurate.

- [HTTP Sink connector settings](reference/http-sink-connector.md)
- [Cron Source connector settings](reference/cron-source-connector.md)
- [SSE Source connector settings](reference/sse-source-connector.md)
- [Message formats](reference/message-formats.md)
- [Exchange template syntax](reference/exchange-template-syntax.md)
- [Metrics](reference/metrics.md)

## Understand (explanation)

Discussions that answer "why". They are meant to be read, not followed.

- [Project intention](explanation/project-intention.md) — the problem this project solves.
- [Architecture](explanation/architecture.md) — how the sink connector processes a message.
- [Request handling](explanation/request-handling.md) — async execution, rate limiting, and retries.
- [Publish modes](explanation/publish-modes.md) — NONE and PRODUCER.
- [Scalability](explanation/scalability.md) — how load is distributed across tasks and workers.
