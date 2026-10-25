# Server-advertised rate limits

The HTTP Sink connector honours the IETF `RateLimit` / `RateLimit-Policy` response headers: when a server advertises an exhausted service limit (`r=0` on a `requests` or `concurrent-requests` quota), the next calls to the same target wait for the advertised window before being sent. A window above the configured threshold opens the circuit breaker instead of waiting inline. The local outbound pacing (`rate.limiter.*`) is unchanged and applies on top.

## Sub-features

- `ratelimit-pause` an exhausted `RateLimit` window pauses the next call to the same configuration + target authority (jitter on resume, deadline never shortened).
- `ratelimit-window` the pause is the effective window `t`, else the matching policy's `w`, else the default delay, capped by `rate.limit.headers.max.duration.in.sec`.
- `ratelimit-threshold` a window above `rate.limit.headers.max.threshold.in.sec` opens the circuit breaker instead of waiting inline.
- `ratelimit-unit-filter` only admission units pause (`requests`, `concurrent-requests`); `content-bytes` and unknown units are informational.
- `ratelimit-retry-after-precedence` `Retry-After` / `X-Retry-After` wins when both are present (IETF MUST).
- `ratelimit-disabled` `config.<id>.rate.limit.headers.enabled=false` restores the previous behaviour.

## How to get to it (user POV)

- Register the connector: `connector.class: io.github.clescot.kafka.connect.http.sink.HttpSinkConnector`.
- Every configuration id accepts the `config.<id>.rate.limit.headers.*` settings: `enabled` (default `true`), `scope` (`instance`/`static`), `max.threshold.in.sec` (`60`), `max.duration.in.sec` (`86400`), `default.delay.in.sec` (`3600`).
- Produce `HttpRequest` messages, on a dedicated topic, pointing at a server that answers with the `RateLimit` field.

## Driving it with control-kchttp

Preconditions:

- A run is up and `control-kchttp doctor` passes.
- No connector named `http-sink-ratelimit` is already registered.

The whole recipe is one command; the bullets below are what it does and what proves each step.

```bash
.agents/skills/verify-kafka-connect-http/scripts/control.sh drive-ratelimit
```

- **Use a dedicated topic.** The driver creates `ratelimit-request` and registers its sink on it, so other recipes' sink connectors (which stay registered on `http-request` after e.g. `drive-sink`) cannot re-send the same records and pollute the spacing proof.
- **Stub the target.** Register `POST /limited -> 200` with the header `RateLimit: "default";r=0;t=5` on the target. The driver does this and writes the mapping response to the run's artifacts.
- **Register the connector.** PUT `http-sink-ratelimit` with `topics: ratelimit-request`, `publish.mode: NONE` and the `StringConverter`s. Defaults are enough: the feature is enabled by default and the 5 s window is below the 60 s threshold, so the gate waits inline.
- **Require RUNNING.** `connector.state` and `tasks[0].state` must both read `RUNNING`.
- **Produce three requests, one at a time.** Each record is produced as soon as the previous one has been recorded by the target, so the producer contributes no spacing of its own; any gap is the connector's pause. (Producing a burst at once would not prove it: the sink dispatches records concurrently, before the first response has advertised the exhausted quota.)
- **Observe the pacing.** A poll of `GET http://127.0.0.1:8080/__admin/requests` timestamps each request when it is first seen (WireMock's request log carries no timestamps); consecutive sightings are about 5 s apart (the advertised `t`), not milliseconds.
- **Proof.** The driver writes `ratelimit-config.json`, `ratelimit-config-response.json`, `ratelimit-requests.json`, `ratelimit-dates.txt`, `ratelimit-gaps-ms.txt`, `ratelimit-connect-logs.txt` and `ratelimit-SUMMARY.md` under `artifacts/verify-kafka-connect-http/<RUN_ID>/`, and fails unless the smallest gap is at least 4 s.
- **Verified.** Run `20261026-001116`: the connector and its task reached `RUNNING/RUNNING`, three `POST /limited` requests were recorded with gaps of 5199 ms and 5123 ms for the advertised `t=5`, and the worker log shows `waiting '<n>' ms for the advertised rate limit window of 'http://http-target:8080'`. Evidence: `artifacts/verify-kafka-connect-http/20261026-001116/`.

## Gotchas

- The pause is per configuration + target authority: another host is not delayed by host A's quota.
- `Retry-After` / `X-Retry-After` always wins over `RateLimit`: a response carrying both follows `Retry-After`.
- The stub must quote the policy name (`"default"`); an item without `r` is ignored (the draft requires tolerant parsing, malformed fields are silently dropped).
- A service limit without a matching `RateLimit-Policy` is treated as unit `requests`.
- Jitter can extend a pause by up to 500 ms, so gaps exceed `t` rather than matching it exactly; the driver therefore asserts a floor (4 s for a 5 s window), not equality.
- The circuit-breaker side (over-threshold windows) is observable by advertising a window above 60 s: calls get a synthetic 500 until the breaker half-opens.
- The pause applies before every attempt, retries of the same exchange included, and waits are recorded on the exchange as the `rateLimitWaitingTime` timing.
