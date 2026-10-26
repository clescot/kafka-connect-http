# Troubleshoot

Solutions to the problems operators hit most often. Each entry states the symptom, the usual
cause, and the fix.

## The connector fails to start or validate

**Symptom:** the connector returns a configuration error listing an unknown property.

**Cause:** a property name that does not exist — often a leftover from an older version.

**Fix:** check the property against the [reference](../reference/http-sink-connector.md).
Two common mistakes:

- There is no `publish.to.in.memory.queue`. Publishing is controlled by `publish.mode`
  (`NONE` or `PRODUCER`).
- Configuration settings use the `config.<id>.` prefix, not `httpclient.<id>.`. The default
  configuration is `config.default.`.

## Requests are sent but no responses appear

**Symptom:** the HTTP requests reach the server, but the success/error topics stay empty.

**Cause:** `publish.mode` defaults to `NONE`, which discards responses.

**Fix:** set `publish.mode` to `PRODUCER`. See
[choose a publish mode](choose-a-publish-mode.md).

## No HTTP request is made for a message

**Symptom:** messages are consumed but no request leaves the connector.

**Cause:** no `HttpRequestMapper` matched the message, or a configuration predicate sent it
somewhere unexpected.

**Fix:**

- A `default` mapper catches every message by default; do not remove its catch-all matcher.
  Check `http.request.mapper.<id>.matcher` on your custom mappers.
- For the `JEXL` mode, verify the expressions extract a `url`; a message without a valid
  `url` cannot become a request.
- Inspect the connector logs at `DEBUG`; the connector logs the chosen configuration and
  mapper.

## Messages are repeatedly retried, then fail

**Symptom:** the same request is attempted several times, then lands in the error topic.

**Cause:** the response code matched the retry regex and the retry budget ran out.

**Fix:** retries are governed by `config.<id>.retry.policy.*`:

- `retry.policy.response.code.regex` (default `^5[0-9][0-9]$`) selects retryable codes.
- `retries`, `retry.delay.in.ms`, `retry.max.delay.in.ms`, `retry.delay.factor`,
  `retry.jitter.in.ms` size the retry budget.

If a server sends `Retry-After`, the `retry.after.*` settings decide whether the connector
waits locally or opens the circuit breaker.

## Requests are throttled too aggressively (or not at all)

Adjust the rate limiter: `config.<id>.rate.limiter.max.executions` over
`config.<id>.rate.limiter.period.in.ms`. Set
`config.<id>.rate.limiter.permits.per.execution` to `request_length` to meter by request
size. Use `rate.limiter.scope: static` to share one limiter across connector instances in
the same JVM.

## The connector pauses calls after a server advertises an exhausted quota

**Symptom:** after a successful response, the next requests to that host wait (the log reads
`waiting '...' ms for the advertised rate limit window of '...'`).

**Cause:** the server advertised an exhausted IETF `RateLimit` service limit (`r=0`); the
connector respects the advertised window instead of running into 429s.

**Fix:** let it wait, or tune `config.<id>.rate.limit.headers.*`: `enabled: false` restores the
previous behaviour, `max.threshold.in.sec` bounds the inline wait (above it the circuit breaker
opens), `max.duration.in.sec` caps the advertised window. See
[server-advertised rate limits](../reference/http-sink-connector.md#server-advertised-rate-limits).

## SSL handshake failures

**Symptom:** `SSLHandshakeException` against a server with a private certificate authority.

**Fix:** provide a trust store via `config.<id>.httpclient.ssl.truststore.*`. Do not reach
for `httpclient.ssl.truststore.always.trust` in production — it disables verification
entirely. See [configure authentication](configure-authentication.md#custom-ssl-material).

## Cron jobs never fire

**Symptom:** the Cron Source connector starts but emits nothing.

**Fix:**

- Job settings use the `job.<jobId>.` prefix: `job.job1.url`, `job.job1.cron`,
  `job.job1.method`, `job.job1.body`, `job.job1.headers`, `job.job1.header.<Name>`.
- Every id in `jobs` must have a matching `job.<id>.url` and `job.<id>.cron`.
- Confirm the [Quartz cron expression](https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html)
  — it has six or seven fields, not the five of Unix cron.

## SSE stream stops after an error

**Symptom:** the SSE source stops delivering events after a transient failure.

**Fix:** choose an error strategy with `config.<id>.error.strategy`:
`always-continue`, `always-throw`, `continue-with-max-attempts`, or
`continue-with-time-limit`. Tune backoff with the `retry.delay.strategy.*` settings.

## The SSE source is not reconnecting

**Symptom:** the SSE source stopped logging connection attempts and delivers nothing, with no
task failure.

**Fix:** the server may have asked the connector to wait. Look for
`delaying the next SSE connection attempt to '<authority>' by '<n>' ms` and
`the SSE server at '<authority>' asked to wait '<n>' s before reconnecting` in the worker log.
The pause honours an exhausted `RateLimit` window or a `Retry-After`, whichever came last, and is
capped by `config.<id>.rate.limit.headers.max.duration.in.sec` and
`config.<id>.retry.policy.retry.after.max.duration.in.sec` (both default to a day). A window that
long means the connector is behaving as asked, not hanging. To decline, set
`config.<id>.rate.limit.headers.enabled: false` and, if the delay came from `Retry-After`, narrow
`config.<id>.retry.policy.retry.after.status.code`. Note that
`config.<id>.rate.limit.headers.max.threshold.in.sec` does **not** shorten the wait on the source:
with no circuit breaker, it only selects the log level.

`stop()` interrupts the wait immediately, so a paused task still shuts down cleanly.

## I cannot see what the connector is doing

**Fix:** enable metrics. Activate an exporter
(`meter.registry.exporter.jmx.activate` or `meter.registry.exporter.prometheus.activate`)
and the HTTP client timers become available. See [metrics](../reference/metrics.md).

## Next steps

- [HTTP Sink reference](../reference/http-sink-connector.md)
- [Publish modes explained](../explanation/publish-modes.md)
- [Request handling](../explanation/request-handling.md)
