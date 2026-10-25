# SSE Source connector

The SSE Source connector opens Server-Sent-Events streams (one per configured URL) and emits each received event as a Kafka message into the configured topic. It is a source-only feature: events flow SSE stream -> Kafka, with OkHttp handling the SSE protocol.

## Sub-features

- `sse-register` registers the connector; it reaches `RUNNING/RUNNING`.
- `sse-connect` establishes the SSE connection to each configured URL.
- `sse-emit` each event on the stream becomes a Kafka message in the configured topic.
- `sse-resume` a restarted task asks the server to resume from the last committed event id, so events published while it was down are re-delivered instead of skipped.
- `sse-multi-config` multiple configurations (`config.ids`) each map their URL to their own topic.
- `sse-template` `config.<id>.exchange.template` rewrites each event's data before it is emitted (the event id, type and key are preserved); `config.<id>.exchange.template.processors` restricts the enabled processors.

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
  Records are the SSE events, for example `{"id":"1","attributes":"{}","type":"one","data":"first"}`; assert on `type`, `data` and `id`.
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
- **Verified.** Runs `20261025-202706` (register/connect/emit/resume) and `20261025-231240` (all six sub-features). In the latter: registration answered HTTP 201 without a top-level `topic`, 3 stub records (ids 1–3) were consumed from `sse-events`, and after `POST .../tasks/0/restart` the first new request to `/events` carried `last-event-id: 3` with the log line `resuming SSE configuration default from the committed event id 3`; `config.ids: default,second` routed the second stream's events only to `sse-events-2`; and `config.default.exchange.template: tpl:${jsonpath:$.eventType}:${jsonpath:$.eventId}` rewrote `data` to `tpl:time:<server id>` while the record `id` stayed the server id. Evidence: `artifacts/verify-kafka-connect-http/20261025-231240/sse/`.

## Gotchas

- `sse-target` is a scratch image with no shell/curl, so it has no Docker healthcheck; check it from the host (`http://127.0.0.1:8082/`) rather than expecting `(healthy)`.
- Configure the connector with the **in-network** URL `http://sse-target:8080/.sse`. The host port 8082 is only for the manual reachability check.
- `config.default.url` and `config.default.topic` are mandatory, even when `config.ids` lists only other ids: the connector validates the default pair at start-up and refuses to start without it.
- A top-level `topic` is not read by this connector — it is filtered out before the task starts; only `config.<id>.topic` routes events.
- The stream never ends and ids keep incrementing, so a consumer may see different events each run — assert on the event shape, not on a specific id or count.
- SSE events are only emitted while the connection is up; consume quickly after registering, or use `--from-beginning` on the topic.
- WireMock lowercases header names in `/__admin/requests`, so look up `last-event-id`, not `Last-Event-ID`, and its request objects carry no timestamp. The array is newest first, so record a count or the request ids before the restart to find the boundary.
- `DELETE /connectors/<name>` did not clear the committed offsets in this stack: a connector re-created under the same name resumed from its previous position. To reach the cold-start branch, use a name that has never run.
- Every log line the plugin writes appears twice with the same timestamp in this stack, while Kafka's own lines appear once. A single task start therefore logs its resume point twice, and the `SseBackgroundEventHandler` errors double the same way (run `20261025-213548`). This is the logging setup in the image, not two connections, so do not count those lines to infer how many streams run.
- The library reconnects whenever a stubbed body ends, and `sse-target` never ends its stream, so the target records one request per reconnect and the topic receives duplicates. Duplicates are allowed by the promise this connector makes, which is no loss.
- `request` events echo the HTTP request verbatim, newlines included; the record value is JSON with those newlines escaped (fixed in #955), so parse it as JSON and assert on the parsed `data` field.
