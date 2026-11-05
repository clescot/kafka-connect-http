# kafka-connect-http verification map

This directory is the maintained source for verifying the user-facing behaviour of **kafka-connect-http**. Read this index before driving the app, then use the matching feature file as the recipe.

There is no web UI and no CLI. The surface is Kafka Connect:

- **HTTP Sink connector** — `Kafka -> HTTP`. Consumes `HttpRequest` messages from a topic and performs the HTTP call.
- **Cron Source connector** — emits `HttpRequest` messages into a topic on a Quartz cron schedule.
- **SSE Source connector** — subscribes to Server-Sent-Events streams and emits each event into a topic.

The harness is `scripts/control.sh` (`control-kchttp`), documented in [`../SKILL.md`](../SKILL.md).

## Baseline preconditions

- The plugin image is built: `control-kchttp build` produces `clescot/kafka-connect-http:<version>` from `pom.xml`.
- A run is up: `control-kchttp launch`. It brings up `kafka` (cp-kafka 8.0.0, KRaft), `connect` (this repo's image with the plugin installed) and the two HTTP targets, then creates topics `http-request`, `http-success`, `http-error`.
- `control-kchttp doctor` passes (containers up, all three connector classes present in `GET /connector-plugins`, both targets healthy, port 8083 owned by our Connect worker).
- Artifacts go to `artifacts/verify-kafka-connect-http/<RUN_ID>/`. The current run id is in `artifacts/verify-kafka-connect-http/.current-run`.
- Never drive a Connect worker this skill did not start.

## Driving conventions

- Every recipe below uses the default host ports: Connect `8083`, target `8080`, SSE target
  `8082`, Kafka `29092`. If you launched with different ports (see *Isolation* in `../SKILL.md`),
  substitute them in the commands you copy.
- Start every recipe from the baseline state unless its preconditions say otherwise.
- Resolve the run's compose project once, then reach into the Kafka container:
  ```bash
  PROJECT="kchttp-$(cat artifacts/verify-kafka-connect-http/.current-run)"
  COMPOSE=(docker compose -f .agents/skills/verify-kafka-connect-http/scripts/compose.yml -p "$PROJECT")
  ```
- Register/update connectors with `PUT http://127.0.0.1:8083/connectors/<name>/config`; require `connector.state` **and** `tasks[0].state` to be `RUNNING` before driving records.
- Connector configs use **in-network** hostnames (`kafka:9092`, `http-target:8080`, `sse-target:8080`), because the Connect worker calls them container-to-container. Host-side tooling uses `127.0.0.1`.
- Keep the pipeline string-based with `"value.converter": "org.apache.kafka.connect.storage.StringConverter"` so no Schema Registry is needed.
- Use unique connector names per recipe; delete them when finished so later recipes start clean.
- Treat every command as literal; keep quoted names and flags unchanged.
- Do not remove proof artifacts during cleanup.

## Proof and skip reporting

- Capture the user action and the resulting state, not only the final screen.
- Sink proof = the request the HTTP target **recorded** (from `GET http://127.0.0.1:8080/__admin/requests`: method, `absoluteUrl`, headers, body), plus connector `RUNNING/RUNNING`. The connector starting is not proof the call happened.
- Source proof = a record present in the destination topic, read back with `kafka-console-consumer`, with its body asserted.
- Verify side effects (Kafka records, HTTP requests recorded) alongside connector state.
- Record the feature ID and entry point used with every artifact.
- Report an unreachable path with the attempted command and the unmet precondition.
- Do not report a skipped entry point as verified through a different path.

## Feature entry contract

Each feature file starts with an H1 title and one paragraph describing the user-visible behaviour. It then uses exactly four H2 sections in this order.

1. `Sub-features` lists short IDs with one line for each behaviour.
2. `How to get to it (user POV)` lists every user entry point.
3. `Driving it with control-kchttp` starts with `Preconditions:` and uses labeled bullets that pair each user action with an exact command and observable result.
4. `Gotchas` lists traps that can waste or invalidate a verification run.

Keep implementation details out of the map. Name only user paths, stable handles, required state, commands, and observable proof.

## Status of the seed map

Verified by live runs: **HTTP Sink connector** (`http-sink-connector.md`, via `control-kchttp drive-sink`), **Cron Source connector** (`cron-source-connector.md`, run `20261025-231240`), **SSE Source connector** (`sse-source-connector.md`, run `20261025-231240`, covering resume, multi-config and template; its advertised-delay pacing is scripted, run `20261026-013419`), and **Server-advertised rate limits** (`rate-limit-headers.md`, run `20261026-001116`, via `control-kchttp drive-ratelimit`). The SSE recipe is driven by hand except for the pacing, which has a driver (`control-kchttp drive-sse-ratelimit`).

Re-driven end to end on `20261029-210442`: `drive-sink` (DIRECT mapping), a JEXL-mapping drive by
hand, `drive-ratelimit` plus a threshold probe, the cron and SSE recipes by hand, and
`drive-sse-ratelimit`. That run also proved the record shape, the resume header and multi-config
routing quoted in the SSE recipe, and showed that WireMock preserves header case.

The sink JEXL date-time recipe was re-driven on `20261029-225830`: a record with a pinned timestamp
produced a `POST /ping` whose URL, `X-Event-Time` header and body all carry that event time.
Evidence: `artifacts/verify-kafka-connect-http/20261029-225830/jexl/`.

Each recipe still lists what no run has covered, in its own file. Do not move a bullet to verified without a run that produced the record or request it describes.

## Features

- [HTTP Sink connector](./http-sink-connector.md) — `Kafka -> HTTP`: a topic message triggers an HTTP request; the target records it.
- [Server-advertised rate limits](./rate-limit-headers.md) — the sink waits when a server advertises an exhausted IETF `RateLimit` window; the SSE source's equivalent pacing lives in [the SSE recipe](./sse-source-connector.md) and is driven by `control-kchttp drive-sse-ratelimit`.
- [Cron Source connector](./cron-source-connector.md) — scheduled `HttpRequest` emission into a topic.
- [SSE Source connector](./sse-source-connector.md) — Server-Sent-Events stream -> Kafka messages.
