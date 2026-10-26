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

## Server-advertised delays

The connector honours the delays a server advertises in the responses to its connection attempts
(including the refused ones). A pause delays the next connection or reconnect attempt to the same
target authority; it never drops an event, and the stream resumes from the same cursor as before.

It reads two sources, with the same interpretation as the HTTP sink (see
[ADR 0003](../adr/0003-pace-sse-source-with-ratelimit-headers.md)):

* the IETF `RateLimit` / `RateLimit-Policy`
  ([draft-ietf-httpapi-ratelimit-headers](https://datatracker.ietf.org/doc/draft-ietf-httpapi-ratelimit-headers/)).
  When a response advertises an exhausted admission quota (`r=0` on a `requests` or
  `concurrent-requests` unit), the next attempt to that authority waits for the advertised window.
  Malformed fields, items without `r`, and non-admission units are ignored.
* `Retry-After` / `X-Retry-After`, honoured on the status codes matching
  `config.<id>.retry.policy.retry.after.status.code`. It wins when both are present, and the
  `RateLimit` field is then ignored.

| Setting | Default | Description |
| --- | --- | --- |
| `config.<id>.rate.limit.headers.enabled` | `true` | Honour the `RateLimit` fields at all. `false` restores the previous behaviour. |
| `config.<id>.rate.limit.headers.scope` | `instance` | `instance` (per configuration in this connector) or `static` (one gate per configuration id shared across connector instances in the JVM). |
| `config.<id>.rate.limit.headers.max.threshold.in.sec` | `60` | **Advisory for this connector** (see below): above this window the pause is logged at WARN instead of INFO. |
| `config.<id>.rate.limit.headers.max.duration.in.sec` | `86400` | Maximum advertised `RateLimit` window considered; longer values are capped. |
| `config.<id>.rate.limit.headers.default.delay.in.sec` | `3600` | Delay used when an exhausted service limit has neither `t` nor a matching policy `w`. |
| `config.<id>.retry.policy.retry.after.status.code` | `503\|429\|301` | Status codes whose `Retry-After` is honoured. |
| `config.<id>.retry.policy.retry.after.max.duration.in.sec` | `86400` | Maximum `Retry-After` delay considered; longer values are capped. |

Two behaviours are worth knowing:

* The pause stays under the control of the underlying event source, so `stop()` interrupts a wait
  immediately instead of holding the task for the length of the advertised window.
* An advertised delay is never shorter than the retry delay strategy's own backoff, and can be
  longer than `config.<id>.retry.delay.strategy.max-delay-millis`: a server explicitly asking the
  connector to stay away outranks the local backoff cap.

## Templating

| Setting | Default | Description |
| --- | --- | --- |
| `config.<id>.exchange.template` | `""` | Template applied to each event. Empty disables templating. |
| `config.<id>.exchange.template.processors` | `""` | Comma-separated allow-list of processors. Empty allows all built-ins. |

See [customize exchange templates](../how-to/customize-exchange-templates.md).

## Resuming after a restart or a rebalance

Each configuration's progress is tracked as a Kafka Connect source offset: the `id:` of the last
SSE event committed for that configuration. When a task starts, it reads that offset back and asks
the server to resume after it with a `Last-Event-ID` request header, so events published while the
connector was down are re-delivered instead of skipped.

Two consequences are worth knowing:

* The offset is keyed by the configuration id, so **changing a configuration id starts a new
  stream**, with no committed position, under the server's own default policy.
* A configuration with no committed offset has nothing to ask for. That covers a first-ever start,
  and the first start after upgrading from a version that keyed offsets by event type, whose old
  offsets are orphaned. The connector sends no `Last-Event-ID` and inherits the server's default
  policy, which for most servers means "from now", so events published before its first commit are
  not recoverable. It logs `no committed offset for configuration <id>` when that happens.

The cursor is the `id:` the server sent, not the id an `exchange.template` may have produced, so a
template that rewrites or drops the id does not move the resume point.

Whether the server honours `Last-Event-ID` is the server's decision. The connector asks, and a
server that ignores the header, or that treats an unknown id as "from now", will still skip events
published during the gap. The connector cannot detect that.

## Known inconsistencies

The following are genuine mismatches between the setting definitions and their documented
text. They are listed so you can confirm the effective behaviour against your build:

* `config.<id>.rate.limit.headers.max.threshold.in.sec` shares its description with the sink,
  where an over-threshold window opens the circuit breaker. The SSE source has no circuit
  breaker: an over-threshold window is still waited out (capped by the `max.duration` settings),
  and the threshold only changes the log level.
* `config.<id>.retry.delay.strategy.max-delay-millis` is documented as the maximum delay between
  retries, but it does not cap an advertised pause: `RateLimit` windows are capped by
  `rate.limit.headers.max.duration.in.sec` and `Retry-After` by
  `retry.policy.retry.after.max.duration.in.sec`.

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
