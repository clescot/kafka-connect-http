# SseSourceConnector

Server Sent Event (SSE) Source connector permits to receive events and convert them as kafka messages.
Multiple event streams can be configured with for each one its own configuration, with its `URL` and `topic`.
SSE Connector is relying on [OkHttp](https://square.github.io/okhttp/) to handle the SSE protocol.
So, many configuration options present in the HTTP Connector, are also available here to tune the connection, 
like inserting custom headers, configuring timeouts, etc.
Unlike the HTTP Sink connector which select the right configuration against the current HTTP request, 
the SSE Source connector is connecting to all configured URLs owned by configurations, listening to events 
and provide them to each configured topic.

## required parameters

* `url` will receive SSE messages emitted
* `topic` will receive SSE messages emitted


## optional parameters

* `retry.delay.strategy.max-delay-millis` (default: `30000`) - Maximum delay in milliseconds between retries.
* `retry.delay.strategy.backoff-multiplier` (default: `2`) - Multiplier for the backoff delay.
* `retry.delay.strategy.jitter-multiplier` (default: `0.5`) - Jitter multiplier for the backoff delay.
* `error.strategy` (default: `alwaysThrow`) - Strategy to use when an error occurs. Possible values are 
`always-throw`, `always-continue`, `continue-with-max-attempts`,`continue-with-time-limit`.
* `error.strategy.max-attempts` (default: `3`) - Maximum number of attempts before giving up.
* `error.strategy.time-limit-millis` (default: `60000`) - Time limit in milliseconds before giving up.
* `exchange.template` - template applied to each received SSE event. empty by default (templating disabled).
* `exchange.template.processors` - comma-separated list of template processors to enable. when empty, all
  built-in processors are enabled. when set, only the listed processors are enabled (**whitelist**) and unknown
  names are rejected at connector configuration validation.
  See [Exchange Template Customization](EXCHANGE_TEMPLATE_EXAMPLES.md).

Both template settings are available per configuration, e.g. `config.default.exchange.template` and
`config.default.exchange.template.processors`.

## resuming after a restart or a rebalance

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
  not recoverable. It logs `no committed offset for <configuration>` when that happens.

Whether the server honours `Last-Event-ID` is the server's decision. The connector asks, and a
server that ignores the header, or that treats an unknown id as "from now", will still skip events
published during the gap. The connector cannot detect that.

## minimal configuration example

```json
{
   "tasks.max" : "1",
   "connector.class" : "io.github.clescot.kafka.connect.sse.client.okhttp.SseSourceConnector",
   "topic" : "sse_events",
   "key.converter" : "org.apache.kafka.connect.storage.StringConverter",
   "value.converter" : "org.apache.kafka.connect.storage.StringConverter",
   "config.default.url":"http://mywebsite.com/sse",
   "config.default.topic":"sse_events"
}
```


## example for a default configuration, with a static header, a connect timeout, a retry strategy, and an error strategy.

```json
{
   "tasks.max" : "1",
   "connector.class" : "io.github.clescot.kafka.connect.sse.client.okhttp.SseSourceConnector",
   "topic" : "sse_events",
   "key.converter" : "org.apache.kafka.connect.storage.StringConverter",
   "value.converter" : "org.apache.kafka.connect.storage.StringConverter",
   "config.default.url":"http://mywebsite.com/sse",
   "config.default.topic":"sse_events",
   "config.default.enrich.request.static.header.names": "auth1",
   "config.default.enrich.request.static.header.auth1": "value1",
   "config.default.okhttp.connect.timeout": "3000",
   "config.default.retry.delay.strategy.max-delay-millis": "5000",
   "config.default.retry.delay.strategy.backoff-multiplier": "1.5",
   "config.default.error.strategy":"continue-with-max-attempts",
   "config.default.error.strategy.max-attempts":"4"
}
```