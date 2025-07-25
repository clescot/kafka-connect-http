# SseSourceConnector

Server Sent Event (SSE) Source connector permits to receive events and convert them as kafka messages.
Multiple event streams can be configured with for each one its own configuration, with its `URL` and `topic`.


## required parameters

* `url` will receive SSE messages emitted
* `topic` will receive SSE messages emitted


## optional parameters

* `retry.delay.strategy.max-delay-millis` (default: `30000`) - Maximum delay in milliseconds between retries.
* `retry.delay.strategy.backoff-multiplier` (default: `2`) - Multiplier for the backoff delay.
* `retry.delay.strategy.jitter-multiplier` (default: `0.5`) - Jitter multiplier for the backoff delay.
* `error.strategy` (default: `alwaysThrow`) - Strategy to use when an error occurs. Possible values are `always-throw`, `always-continue`, `continue-with-max-attempts`,`continue-with-time-limit`.
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