# SSE Source connector

The SSE Source connector opens Server-Sent-Events streams (one per configured URL) and emits each received event as a Kafka message into the configured topic. It is a source-only feature: events flow SSE stream -> Kafka, with OkHttp handling the SSE protocol.

## Sub-features

- `sse-register` registers the connector; it reaches `RUNNING/RUNNING`.
- `sse-connect` establishes the SSE connection to each configured URL.
- `sse-emit` each event on the stream becomes a Kafka message in the configured topic.
- `sse-multi-config` multiple configurations (`config.ids`) each map their URL to their own topic.
- `sse-template` `config.<id>.exchange.template` rewrites each event before it is emitted.

## How to get to it (user POV)

- Register the connector: `connector.class: io.github.clescot.kafka.connect.sse.client.okhttp.SseSourceConnector`.
- Set `topic` and, per configuration, `config.default.url` and `config.default.topic`.
- Events then appear on the topic.

## Driving it with control-kchttp

Preconditions:

- A run is up and `control-kchttp doctor` passes; since `sse-target` is part of the stack, `doctor` reports it.
- Topic `sse-events` exists — create it: `"${COMPOSE[@]}" exec -T kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic sse-events --partitions 1 --replication-factor 1`.
- The `sse-target` container (`jmalloc/echo-server:v0.3.7`) serves a perpetual SSE stream at `http://sse-target:8080/.sse`, with incrementing `id:` values and one `server` event plus a `request` event per connection.
- No connector named `sse-source-verify` is already registered.

```bash
PROJECT="kchttp-$(cat artifacts/verify-kafka-connect-http/.current-run)"
COMPOSE=(docker compose -f .agents/skills/verify-kafka-connect-http/scripts/compose.yml -p "$PROJECT")
```

- **Confirm the stream is reachable from the host.** `curl -s --max-time 3 http://127.0.0.1:8082/.sse | head`. Output contains `event: server`, `event: request` and `id:` lines.
- **Register the connector.** `PUT http://127.0.0.1:8083/connectors/sse-source-verify/config` with:
  ```json
  {
    "connector.class": "io.github.clescot.kafka.connect.sse.client.okhttp.SseSourceConnector",
    "tasks.max": "1",
    "topic": "sse-events",
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
  Records are the SSE events; assert on their content (e.g. an `event`/`data` shape with an `id`).
- **Observe the connection.** `control-kchttp logs connect` shows the SSE connection to `http://sse-target:8080/.sse`. This is supporting evidence only.
- **Proof.** Capture the connector status, a couple of consumed `sse-events` records, and the connect log excerpt into `artifacts/verify-kafka-connect-http/<RUN_ID>/sse/`.

## Gotchas

- `sse-target` is a scratch image with no shell/curl, so it has no Docker healthcheck; check it from the host (`http://127.0.0.1:8082/`) rather than expecting `(healthy)`.
- Configure the connector with the **in-network** URL `http://sse-target:8080/.sse`. The host port 8082 is only for the manual reachability check.
- `config.default.url` / `config.default.topic` are needed even for the default configuration: the connector connects to every URL owned by a configuration, and a URL-less configuration connects to nothing.
- The stream never ends and ids keep incrementing, so a consumer may see different events each run — assert on the event shape, not on a specific id or count.
- SSE events are only emitted while the connection is up; consume quickly after registering, or use `--from-beginning` on the topic.
- Not yet exercised by a verification run: this recipe is derived from `docs/sse_source_connector_settings.md` and `ITSseSourceTaskTest`. Run it once and record the artifacts before treating it as verified.
