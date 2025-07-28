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