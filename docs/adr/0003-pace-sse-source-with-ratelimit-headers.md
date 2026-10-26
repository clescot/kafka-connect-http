# Pace the SSE source with the advertised RateLimit headers

The HTTP sink waits out an exhausted quota advertised in the IETF `RateLimit` /
`RateLimit-Policy` response fields (ADR 0002). The SSE source does not go through that
machinery: launchdarkly-eventsource owns the connect/reconnect loop, so the server's advertised
quota had no influence on when the source reconnects or how fast it retries.

The source now reads the same fields from every connection attempt. A `RateLimitGate` — the
sink's, extended with a header-map overload and a non-blocking `remainingPauseMillis` — is keyed
by configuration + target authority, exactly as in the sink. Observation happens in a
`ConnectStrategy` wrapper, which sees both the headers of an accepted stream
(`Client.Result.getHeaders()`) and those of a refused one (`StreamHttpErrorException.getHeaders()`).
The pause is not slept by the connector: a `RetryDelayStrategy` wrapper returns
`max(libraryDelay, advertisedRemaining)`, so the wait happens inside the library's own interruptible
sleep.

## Consequences

- The wait is owned by the library, so `stop()` still wakes the stream immediately. Sleeping
  inside `connect()` would block task shutdown for the length of the advertised window.
- `Retry-After` / `X-Retry-After` wins when present on a status code the configuration honours
  (`config.<id>.retry.policy.retry.after.status.code`, default `503|429|301`); the `RateLimit`
  field is then ignored, as the draft requires. An advertised delay never shortens the library's
  own backoff.
- The source has no circuit breaker, so `rate.limit.headers.max.threshold.in.sec` is advisory
  here: an over-threshold window is waited out rather than opening a breaker, and the threshold
  only chooses the log level. `rate.limit.headers.max.duration.in.sec` (RateLimit) and
  `retry.policy.retry.after.max.duration.in.sec` (Retry-After) are the ceilings, and neither is
  clamped by `retry.delay.strategy.max-delay-millis`: a server asking us to stay away for longer
  outranks the connector's local backoff cap.
- Only the timing changes. The `Last-Event-ID` seed, the queue, the offsets and the poll loop are
  untouched, so the at-least-once promise the TLA+ check models is unaffected: a pause is a
  stutter step, during which the server may publish more events, and the next connection resumes
  from the cursor as before.

## Considered options

- **Opening a gate before `connect()`.** Closest to the sink's "before every call" and would pace
  a `static`-scope deadline carried over from a previous task in the same JVM, but the sleep would
  be an uninterruptible `Thread.sleep` inside the library's connect callback: a long window would
  hang `stop()`. Rejected.
- **Observing headers in `SseBackgroundEventHandler.onMessage`.** Simpler to unit-test, but the
  same headers arrive on every event of the connection, so each event would push the deadline
  forward by another full window; and refused connections — the case that matters — never reach
  the handler's message path. Rejected.
- **Duplicating the gate in the SSE package.** Would let the sink's "newer deadline wins, healthy
  quota never cancels" state machine drift between two connectors. Rejected in favour of extending
  `RateLimitGate`.
- **Modelling the pause in the TLA+ check.** The model has no time variable and `NoGap` is a state
  invariant, so a pause is already an allowed stutter step; adding a named action would not
  strengthen the property. The pause is recorded as out of scope (timing, not delivery) instead.
