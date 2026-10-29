# SSE Source connector

The SSE Source connector opens Server-Sent-Events streams (one per configured URL) and emits each received event as a Kafka message into the configured topic. It is a source-only feature: events flow SSE stream -> Kafka, with OkHttp handling the SSE protocol.

## Sub-features

- `sse-register` registers the connector; it reaches `RUNNING/RUNNING`.
- `sse-connect` establishes the SSE connection to each configured URL.
- `sse-emit` each event on the stream becomes a Kafka message in the configured topic.
- `sse-resume` a restarted task asks the server to resume from the last committed event id, so events published while it was down are re-delivered instead of skipped.
- `sse-multi-config` multiple configurations (`config.ids`) each map their URL to their own topic.
- `sse-template` `config.<id>.exchange.template` rewrites each event's data before it is emitted (the event id, type and key are preserved); `config.<id>.exchange.template.processors` restricts the enabled processors.
- `sse-ratelimit-pause` an exhausted `RateLimit` window advertised by a refused connection attempt pauses the next attempt to the same configuration + authority, instead of reconnecting into another 429.
- `sse-ratelimit-precedence` `Retry-After` / `X-Retry-After` wins over the `RateLimit` window when both are present, on the status codes `config.<id>.retry.policy.retry.after.status.code` allows.
- `sse-ratelimit-delivery` a stream whose first attempt was paced still delivers all of its events afterwards (the pause delays attempts, it does not drop events).

The pacing reuses the sink's `RateLimitGate` for the delay: `config.<id>.rate.limit.headers.{enabled,scope,max.duration.in.sec,default.delay.in.sec}` are read. `enabled=false` stops the `RateLimit` pause but leaves the `Retry-After` handling in place. `config.<id>.rate.limit.headers.max.threshold.in.sec` is declared for this connector too, but the pacing does not read it: there is no circuit breaker here and no threshold comparison in the code.

## How to get to it (user POV)

- Register the connector: `connector.class: io.github.clescot.kafka.connect.sse.client.okhttp.SseSourceConnector`.
- Set `config.ids` (optional; `default` is always added) and, per configuration, `config.<id>.url` and `config.<id>.topic` (both required).
- Events then appear on the topic.

## Driving it with control-kchttp

Preconditions:

- A run is up and `control-kchttp doctor` passes; since `sse-target` is part of the stack, `doctor` reports it.
- Topics `sse-events` and (for the multi-config step) `sse-events-2` exist — create each: `"${COMPOSE[@]}" exec -T kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic <topic> --partitions 1 --replication-factor 1`.
- The `sse-target` container (`jmalloc/echo-server:v0.3.7`) serves a perpetual SSE stream at `http://sse-target:8080/.sse`, with incrementing `id:` values, a `server` event and a `request` event per connection, and a `time` event every second.
- `config.ids` is optional: `default` is always added. Each configuration id needs its own `config.<id>.url` and `config.<id>.topic` (both required); the connector validates the `default` pair at start-up even when `config.ids` lists only other ids.
- No connector named `sse-source-verify` is already registered.

```bash
PROJECT="kchttp-$(cat artifacts/verify-kafka-connect-http/.current-run)"
COMPOSE=(docker compose -f .agents/skills/verify-kafka-connect-http/scripts/compose.yml -p "$PROJECT")
```

- **Confirm the stream is reachable from the host.** `curl -s --max-time 3 http://127.0.0.1:8082/.sse | head`. Output contains `event: server`, `event: request` and recurring `event: time` lines, each with an `id:` line.
- **Register the connector.** `PUT http://127.0.0.1:8083/connectors/sse-source-verify/config` with:
  ```json
  {
    "connector.class": "io.github.clescot.kafka.connect.sse.client.okhttp.SseSourceConnector",
    "tasks.max": "1",
    "config.ids": "default",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "config.default.url": "http://sse-target:8080/.sse",
    "config.default.topic": "sse-events"
  }
  ```
  Status reaches `RUNNING/RUNNING`.
- **Observe the emitted events.** Consume a few messages:
  ```bash
  "${COMPOSE[@]}" exec -T kafka kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic sse-events --from-beginning --max-messages 2 --timeout-ms 30000
  ```
  Records are the SSE events as JSON objects, with `attributes` first and an object, not a string:
  `{"attributes":{},"id":"1","type":"one","data":"first"}`. Assert on `type`, `data` and `id`.
- **Observe the connection (supporting evidence).** At the stack's INFO level the plugin logs the resume point (`resuming SSE configuration default from the committed event id <id>`) or the cold start (`no committed offset for configuration <id>`); `control-kchttp logs connect` also carries the raw `connected to SSE server at <url>` line, but that one is DEBUG-only.
- **Observe the resume after a task restart.** `sse-target` ignores `Last-Event-ID`, so stub a stream whose ids you know on the WireMock target and watch the header the restarted task sends:
  ```bash
  # GET /events -> 200, Content-Type: text/event-stream, body "id: 1\n...\n\nid: 2\n...\n\nid: 3\n...\n\n"
  curl -sf -X PUT http://127.0.0.1:8083/connectors/sse-source-verify/config -H 'Content-Type: application/json' \
    -d '{"connector.class":"io.github.clescot.kafka.connect.sse.client.okhttp.SseSourceConnector","tasks.max":"1","config.ids":"default","config.default.url":"http://http-target:8080/events","config.default.topic":"sse-events","key.converter":"org.apache.kafka.connect.storage.StringConverter","value.converter":"org.apache.kafka.connect.storage.StringConverter"}'
  "${COMPOSE[@]}" exec -T kafka kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic sse-events --from-beginning --timeout-ms 15000
  before=$(curl -sf http://127.0.0.1:8080/__admin/requests | jq '.requests|length')
  curl -sf -X POST http://127.0.0.1:8083/connectors/sse-source-verify/tasks/0/restart
  sleep 8
  curl -sf http://127.0.0.1:8080/__admin/requests > after-restart.json
  # the array is newest first, so the restarted task's FIRST request is the oldest new one
  jq -r --argjson n "$before" '.requests[0:((.requests|length) - $n)] | .[-1].request.headers' after-restart.json
  ```
  The first request the target records for the restarted task carries the committed event id in `last-event-id`, and the task logs `resuming SSE configuration default from the committed event id <id>`. A fresh task has no in-memory cursor, so that value can only come from the committed offset. Remember that the log line doubles, as the Gotchas explain.
- **Observe the cold-start branch.** A configuration with no committed offset, meaning a connector name that has never run, sends no resume header and logs `no committed offset for configuration <id>`. That is the documented limit, not a failure of the drive.
- **Observe multi-config routing.** Add a second configuration: `config.ids: default,second`, `config.second.url: http://http-target:8080/sse-second` (stubbed like `/events` above) and `config.second.topic: sse-events-2`. Each topic then receives only its own configuration's events.
- **Observe templating.** Add `config.default.exchange.template: tpl:${jsonpath:$.eventType}:${jsonpath:$.eventId}` and `config.default.exchange.template.processors: jsonpath`. Each record's `data` becomes `tpl:<type>:<server id>` while the record `id` stays the server's.
- **Proof.** Capture the connector status, the consumed records, the target's recorded requests across the restart, and the connect log line naming the resume point into `artifacts/verify-kafka-connect-http/<RUN_ID>/sse/`; the multi-config and template steps write their own subdirectories there.
- **Verified.** Runs `20261025-202706` (register/connect/emit/resume) and `20261025-231240` (all six sub-features). In the latter: registration answered HTTP 201 without a top-level `topic`, 3 stub records (ids 1–3) were consumed from `sse-events`, and after `POST .../tasks/0/restart` the first new request to `/events` carried `last-event-id: 3` with the log line `resuming SSE configuration default from the committed event id 3`; `config.ids: default,second` routed the second stream's events only to `sse-events-2`; and `config.default.exchange.template: tpl:${jsonpath:$.eventType}:${jsonpath:$.eventId}` rewrote `data` to `tpl:time:<server id>` while the record `id` stayed the server id. Re-driven on `20261029-210442`: records came back as `{"attributes":{},"id":"1","type":"one","data":"first"}`, the restarted task sent `last-event-id: 3` with the same log line, `config.ids: default,second` put only `second-*` events in `sse-events-2`, and the template rewrote `data` to `tpl:two:2` while `id` stayed `2`. Evidence: `artifacts/verify-kafka-connect-http/20261029-210442/sse/`.

- **Drive the advertised-delay pacing in one command.** `.agents/skills/verify-kafka-connect-http/scripts/control.sh drive-sse-ratelimit`
  - **Stub two paced endpoints.** The driver registers WireMock scenarios whose *first* attempt answers `429`, then a `200 text/event-stream` with three `id:` frames. `/sse-limited` advertises `RateLimit: "default";r=0;t=5`; `/sse-priority` advertises the same shape with `t=30` **and** `Retry-After: 3`.
  - **Register two configurations.** `config.ids: default,priority`, each endpoint on its own dedicated topic (`sse-ratelimit-default`, `sse-ratelimit-priority`), so neither inherits another recipe's committed offsets.
  - **Observe the pacing.** The driver timestamps the first sighting of attempt 1 and attempt 2 of each endpoint from `GET /__admin/requests` (WireMock's log carries no timestamps) and fails unless `/sse-limited` waited at least 4 s for its 5 s window.
  - **Observe the precedence.** The same measurement on `/sse-priority` must land between 2.5 s and 12 s: `Retry-After: 3` wins, so a gap near 30 s proves the `RateLimit` window was used instead and the driver fails.
  - **Observe that nothing was lost.** Both topics must yield the three events of the accepted stream through `kafka-console-consumer`, so the pause is proven to delay attempts rather than skip events.
  - **Proof.** `sse-ratelimit-config.json`, `sse-ratelimit-config-response.json`, `sse-ratelimit-mapping-<endpoint>-<attempt>.json`, `sse-ratelimit-requests.json`, `sse-ratelimit-requests-final.json`, `sse-ratelimit-dates-<config>.txt`, `sse-ratelimit-gap-<config>-ms.txt`, `sse-ratelimit-consumed-<config>.txt`, `sse-ratelimit-connect-logs.txt` and `sse-ratelimit-SUMMARY.md` under `artifacts/verify-kafka-connect-http/<RUN_ID>/sse-ratelimit/`; the worker log shows `delaying the next SSE connection attempt to ...` and, for `/sse-priority`, `the SSE server at ... asked to wait '3' s before reconnecting`.
  - **Verified.** Run `20261026-013419`: `/sse-limited` paused 5242 ms for the advertised 5 s window, `/sse-priority` waited 2979 ms for a `Retry-After: 3` that beat the 30 s window, and both topics yielded the accepted stream's three events; the worker log carries `delaying the next SSE connection attempt to 'http://http-target:8080' by '5190' ms for the advertised rate limit window` and `... by '2999' ms for the advertised Retry-After`. Re-driven on `20261029-210442`: 5056 ms for the 5 s window and 3006 ms for the `Retry-After: 3` that beat the 30 s window, both streams delivered. Evidence: `artifacts/verify-kafka-connect-http/20261029-210442/sse-ratelimit/`.

## Gotchas

- `sse-target` is a scratch image with no shell/curl, so it has no Docker healthcheck; check it from the host (`http://127.0.0.1:8082/`) rather than expecting `(healthy)`.
- Configure the connector with the **in-network** URL `http://sse-target:8080/.sse`. The host port 8082 is only for the manual reachability check.
- `config.default.url` and `config.default.topic` are mandatory, even when `config.ids` lists only other ids: the connector validates the default pair at start-up and refuses to start without it.
- A top-level `topic` is not read by this connector — it is filtered out before the task starts; only `config.<id>.topic` routes events.
- The stream never ends and ids keep incrementing, so a consumer may see different events each run — assert on the event shape, not on a specific id or count.
- SSE events are only emitted while the connection is up; consume quickly after registering, or use `--from-beginning` on the topic.
- WireMock records header names with the case the connector sent (`X-Mapped` stays `X-Mapped`; the resume header is sent as `last-event-id`), so look up the key in that case. Its request objects carry no timestamp. The array is newest first, so record a count or the request ids before the restart to find the boundary.
- `DELETE /connectors/<name>` did not clear the committed offsets in this stack: a connector re-created under the same name resumed from its previous position. To reach the cold-start branch, use a name that has never run.
- Every log line the plugin writes appears twice with the same timestamp in this stack, while Kafka's own lines appear once. A single task start therefore logs its resume point twice, and the `SseBackgroundEventHandler` errors double the same way (run `20261025-213548`). This is the logging setup in the image, not two connections, so do not count those lines to infer how many streams run.
- The library reconnects whenever a stubbed body ends, and `sse-target` never ends its stream, so the target records one request per reconnect and the topic receives duplicates. Duplicates are allowed by the promise this connector makes, which is no loss.
- `request` events echo the HTTP request verbatim, newlines included; the record value is JSON with those newlines escaped (fixed in #955), so parse it as JSON and assert on the parsed `data` field.
