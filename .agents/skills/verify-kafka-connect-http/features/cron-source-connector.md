# Cron Source connector

The Cron Source connector emits `HttpRequest` messages into a Kafka topic on a Quartz cron schedule. It is a pure Kafka producer of requests: it does not perform the HTTP call itself. Combined with the HTTP Sink connector it lets a pipeline poll an HTTP endpoint on a timer without any external producer.

## Sub-features

- `cron-register` registers the connector over the REST API; it reaches `RUNNING/RUNNING`.
- `cron-fire` emits one message per job on the configured cron expression.
- `cron-method` the emitted `HttpRequest` carries the configured `method`, `url`, `body` and headers.
- `cron-multi-job` multiple jobs (`jobs: job1,job2`) each fire.

## How to get to it (user POV)

- Register the connector: `connector.class: io.github.clescot.kafka.connect.http.source.cron.CronSourceConnector`.
- Set `topic` (the destination) and `jobs` (comma-separated job ids).
- For each job set `job.<id>.url`, `job.<id>.cron`, and optionally `job.<id>.method`, `job.<id>.body`, `job.<id>.headers` + `job.<id>.header.<name>`.

## Driving it with control-kchttp

Preconditions:

- A run is up and `control-kchttp doctor` passes.
- Topic `http-request` exists (created by `launch`).
- No connector named `cron-source-verify` is already registered.

```bash
PROJECT="kchttp-$(cat artifacts/verify-kafka-connect-http/.current-run)"
COMPOSE=(docker compose -f .agents/skills/verify-kafka-connect-http/scripts/compose.yml -p "$PROJECT")
```

- **Register the connector.** `PUT http://127.0.0.1:8083/connectors/cron-source-verify/config` with:
  ```json
  {
    "connector.class": "io.github.clescot.kafka.connect.http.source.cron.CronSourceConnector",
    "tasks.max": "1",
    "topic": "http-request",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "jobs": "job1",
    "job.job1.url": "http://http-target:8080/ping",
    "job.job1.cron": "0/5 * * ? * *",
    "job.job1.method": "POST",
    "job.job1.body": "cron-payload"
  }
  ```
  Status reaches `RUNNING/RUNNING`.
- **Observe the emission.** Consume one message (one cron period is 5 s, so allow up to ~15 s via `--timeout-ms`):
  ```bash
  "${COMPOSE[@]}" exec -T kafka kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic http-request --from-beginning --max-messages 1 --timeout-ms 30000
  ```
  The value is an `HttpRequest` JSON whose `url` is `http://http-target:8080/ping`, `method` is `POST` and `bodyAsString` is `cron-payload`.
- **Observe the end-to-end side effect (with a sink registered).** With `http-sink-connector`'s sink also consuming `http-request`, the target records `POST /ping`. Check `GET http://127.0.0.1:8080/__admin/requests` and assert on a request with body `cron-payload`.
- **Proof.** Capture the connector status and a consumed `http-request` record (and, if a sink is attached, the recorded target request) into `artifacts/verify-kafka-connect-http/<RUN_ID>/cron/`.

## Gotchas

- The cron expression is **Quartz**, not Unix cron: it has 6 or 7 fields (`sec min hour day-of-month month day-of-week [year]`) and `?` is required in either day-of-month or day-of-week. `0/5 * * ? * *` = every 5 seconds. A 5-field Unix expression is rejected.
- The connector only emits; it never calls the target. Without a sink consuming the same topic, nothing HTTP happens — the proof is the Kafka record.
- The message it emits is an `HttpRequest` JSON, not your `job.job1.body` verbatim: `body` is placed in `bodyAsString`.
- Keys are not propagated from jobs; the emitted records have a null key.
- `tasks.max` greater than the number of jobs is harmless, but jobs are grouped: with `tasks.max` lower than the job count, jobs are split across tasks.
- Not yet exercised by a verification run: this recipe is derived from `docs/reference/cron-source-connector.md` and `CronSourceConnector`/`HttpCronSourceTask`. Run it once and record the artifacts before treating it as verified.
