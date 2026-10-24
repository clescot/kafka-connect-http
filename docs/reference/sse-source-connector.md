# SSE Source connector reference

Reference for `io.github.clescot.kafka.connect.sse.client.okhttp.SseSourceConnector`.

This connector connects to one or more [Server-Sent Events](https://html.spec.whatwg.org/multipage/server-sent-events.html)
endpoints and publishes each received event to a Kafka topic. It uses OkHttp for the SSE
protocol, so it also accepts the HTTP client settings described in the
[HTTP Sink reference](http-sink-connector.md#http-client).

## How configurations work

Each endpoint is defined by a **configuration**, identified by an id listed in `config.ids`.
All its settings use the `config.<id>.` prefix. The `default` configuration always exists.

| Setting | Required | Description |
| --- | --- | --- |
| `connector.class` | yes | `io.github.clescot.kafka.connect.sse.client.okhttp.SseSourceConnector` |
| `config.ids` | yes | Comma-separated configuration ids (may include `default`). |
| `config.<id>.url` | yes | SSE endpoint URL. |
| `config.<id>.topic` | yes | Topic to publish events to. |

Unlike the HTTP Sink connector, which selects a configuration per request with a predicate,
the SSE source connector **connects to every configured URL** at once.

## Error handling

| Setting | Default | Description |
| --- | --- | --- |
| `config.<id>.error.strategy` | `always-continue` | `always-continue`, `always-throw`, `continue-with-max-attempts`, or `continue-with-time-limit`. |
| `config.<id>.error.strategy.max-attempts` | `3` | Max attempts, for `continue-with-max-attempts`. |
| `config.<id>.error.strategy.time-limit-count-in-millis` | `60000` | Time limit, in ms, for `continue-with-time-limit`. |

## Retry delay strategy

| Setting | Default | Description |
| --- | --- | --- |
| `config.<id>.retry.delay.strategy.max-delay-millis` | `30000` | Maximum delay between retries, ms. |
| `config.<id>.retry.delay.strategy.backoff-multiplier` | `2` | Backoff multiplier. |
| `config.<id>.retry.delay.strategy.jitter-multiplier` | `0.5` | Jitter multiplier. |

## Templating

| Setting | Default | Description |
| --- | --- | --- |
| `config.<id>.exchange.template` | `""` | Template applied to each event. Empty disables templating. |
| `config.<id>.exchange.template.processors` | `""` | Comma-separated allow-list of processors. Empty allows all built-ins. |

See [customize exchange templates](../how-to/customize-exchange-templates.md).

## Examples

Minimal configuration:

```json
{
  "name": "my-sse-source-connector",
  "config": {
    "connector.class": "io.github.clescot.kafka.connect.sse.client.okhttp.SseSourceConnector",
    "tasks.max": "1",
    "config.ids": "default",
    "config.default.url": "http://mywebsite.com/sse",
    "config.default.topic": "sse_events"
  }
}
```

With a static header, a connect timeout, a retry strategy, and an error strategy:

```json
{
  "name": "my-sse-source-connector",
  "config": {
    "connector.class": "io.github.clescot.kafka.connect.sse.client.okhttp.SseSourceConnector",
    "tasks.max": "1",
    "config.ids": "default",
    "config.default.url": "http://mywebsite.com/sse",
    "config.default.topic": "sse_events",
    "config.default.enrich.request.static.header.names": "auth1",
    "config.default.enrich.request.static.header.auth1": "value1",
    "config.default.okhttp.connect.timeout": "3000",
    "config.default.retry.delay.strategy.max-delay-millis": "5000",
    "config.default.retry.delay.strategy.backoff-multiplier": "1.5",
    "config.default.error.strategy": "continue-with-max-attempts",
    "config.default.error.strategy.max-attempts": "4"
  }
}
```

Multiple endpoints, each with its own template:

```json
{
  "name": "my-sse-source-connector",
  "config": {
    "connector.class": "io.github.clescot.kafka.connect.sse.client.okhttp.SseSourceConnector",
    "tasks.max": "1",
    "config.ids": "config1,config2",
    "config.config1.url": "http://api.example.com/user-events",
    "config.config1.topic": "user-events",
    "config.config1.exchange.template": "${jsonpath:$.content}",
    "config.config1.exchange.template.processors": "jsonpath",
    "config.config2.url": "http://api.example.com/system-metrics",
    "config.config2.topic": "system-metrics",
    "config.config2.exchange.template": "${jsonpath:$.eventType} ${datetime:now:yyyy-MM-dd HH:mm:ss}",
    "config.config2.exchange.template.processors": "jsonpath,datetime"
  }
}
```
