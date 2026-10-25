# In-Memory-Queue Source connector

When the HTTP Sink connector runs with `publish.mode: IN_MEMORY_QUEUE`, it hands each `HttpExchange` (request + response + timing) to an in-process queue. The In-Memory-Queue Source connector drains that queue and publishes the exchanges into a `success.topic` or `error.topic`, so HTTP outcomes become Kafka records. Without a matching source connector on the same Connect worker, the sink's queue has no consumer.

## Sub-features

- `queue-source-register` registers the source connector; it reaches `RUNNING/RUNNING`.
- `queue-success-record` a 2xx exchange arrives in `success.topic` as `HttpExchange` JSON.
- `queue-error-record` a failing exchange arrives in `error.topic`.
- `queue-named` sink and source agree on a non-default `queue.name`.
- `queue-content` `content: response` publishes only the `HttpResponse` instead of the whole exchange.

## How to get to it (user POV)

- Register the sink with `publish.mode: IN_MEMORY_QUEUE` (and a `queue.name`).
- Register a **second** connector, `connector.class: io.github.clescot.kafka.connect.http.source.queue.HttpInMemoryQueueSourceConnector`, on the same Connect worker.
- Point it at the sink with the same `queue.name`, and choose `success.topic` / `error.topic`.
- Produce `HttpRequest` messages onto the sink's topic.

## Driving it with control-kchttp

Preconditions:

- A run is up and `control-kchttp doctor` passes.
- Topics `http-request`, `http-success`, `http-error` exist (created by `launch`).
- No connectors named `http-sink-queue-verify` / `http-source-queue-verify` are already registered.

```bash
PROJECT="kchttp-$(cat artifacts/verify-kafka-connect-http/.current-run)"
COMPOSE=(docker compose -f .agents/skills/verify-kafka-connect-http/scripts/compose.yml -p "$PROJECT")
```

- **Register the source first.** `PUT http://127.0.0.1:8083/connectors/http-source-queue-verify/config` with `connector.class: io.github.clescot.kafka.connect.http.source.queue.HttpInMemoryQueueSourceConnector`, `tasks.max: 1`, `queue.name: verify-queue`, `success.topic: http-success`, `error.topic: http-error`, `value.converter: org.apache.kafka.connect.storage.StringConverter`. Status reaches `RUNNING/RUNNING`. (Registering the source first means the sink finds its consumer and does not wait on `wait.time.registration.queue.consumer.in.ms`.)
- **Register the sink.** `PUT http://127.0.0.1:8083/connectors/http-sink-queue-verify/config` with `connector.class: io.github.clescot.kafka.connect.http.sink.HttpSinkConnector`, `tasks.max: 1`, `topics: http-request`, `publish.mode: IN_MEMORY_QUEUE`, `queue.name: verify-queue`, `value.converter: org.apache.kafka.connect.storage.StringConverter`. Status reaches `RUNNING/RUNNING`.
- **Stub the target.** `POST /ping -> 200` as in `http-sink-connector.md`.
- **Produce a request.** Put an `HttpRequest` for `http://http-target:8080/ping` on `http-request`, exactly as in `http-sink-connector.md`.
- **Observe the record.** Consume the success topic and match on your unique body marker:
  ```bash
  "${COMPOSE[@]}" exec -T kafka kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic http-success --from-beginning --timeout-ms 30000 | grep 'kchttp-queue-verify'
  ```
  With the recipe's `StringConverter` the value is the Connect `Struct.toString()` of the `HttpExchange`, not JSON. Assert on `httpResponse=Struct{statusCode=200` and `httpRequest=Struct{url=http://http-target:8080/ping`, not on `"statusCode":200` and not on a dotted `httpRequest.url` path. For JSON instead, add `"value.converter": "org.apache.kafka.connect.json.JsonConverter"` (and `"value.converter.schemas.enable": "false"` if you want a bare object) to both the sink and the source configs.
- **Proof.** Capture the connector status of both connectors, the produced message, and the consumed `http-success` record into `artifacts/verify-kafka-connect-http/<RUN_ID>/queue/`.

## Gotchas

- The source connector is useless alone: it only reads a queue that a sink in `IN_MEMORY_QUEUE` mode fills. Both must live on the same Connect worker (one `connect` container here).
- Sink and source must share the same `queue.name`; the default queue name is `default`.
- Registering the sink before the source makes the sink wait up to `wait.time.registration.queue.consumer.in.ms` (default 60 s). Register the source first, or lower that setting.
- `tasks.max` and the partition counts of the sink/filter topics must match between sink and source.
- A newly registered sink starts reading `http-request` from earliest under its own consumer group, so records already on the topic, left by an earlier `drive-sink` run for instance, are replayed into `http-success`. Match on a unique body marker instead of taking the first record.
- The record's shape is the `HttpExchange` in `src/test/resources/http-exchange.json`, rendered by the configured value converter; assert on the response status and the request URL, not on `durationInMillis`/`moment`.
- `content: response` changes the published payload to a bare `HttpResponse`; assert against that shape when set.
