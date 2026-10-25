---
name: verify-kafka-connect-http
description: Drive kafka-connect-http the way a user does — register a source/sink connector over the Kafka Connect REST API, push records through Kafka, and prove the HTTP traffic or Kafka records that result. Use when you need to verify a change to the sink, source, cron or SSE connectors, reproduce a runtime bug, or produce evidence that a connector feature works end to end.
---

# Verify kafka-connect-http

kafka-connect-http has no web UI and no CLI of its own. The user-facing surface is **Kafka Connect itself**: you register a connector with a JSON config over the Connect REST API, then observe records flowing `HTTP -> Kafka` (source connectors) or `Kafka -> HTTP` (sink connector). A verification run therefore means: bring up a real Kafka broker + Kafka Connect worker with this repo's plugin installed, register a connector, drive records, and capture what the connector actually did.

The harness lives in this skill. Its one entry point is `scripts/control.sh` (call it `control-kchttp` below). It resolves the repo root itself, so run it from any checkout, worktree or directory. The relative paths below are from the repo root.

Read [`features/README.md`](features/README.md) before driving anything: it indexes the user-facing features and points at the per-feature recipe.

## Launch

The stack is Docker Compose, but you never call `docker compose` by hand — the driver owns run ids, ports and artifacts.

1. Build the plugin jar and the connector image once (needs Docker; takes a few minutes the first time). The image is produced by `dockerfile-maven-plugin` and installs the connector with `confluent-hub` into `/usr/share/confluent-hub-components/clescot-kafka-connect-http-connectors`:

   ```bash
   .agents/skills/verify-kafka-connect-http/scripts/control.sh build
   ```

   This runs `mvn -B -q -pl kafka-connect-http-connectors -am clean package -DskipTests` and produces `clescot/kafka-connect-http:<version>` where `<version>` is read from `pom.xml`.

2. Start the stack and create the topics:

   ```bash
   .agents/skills/verify-kafka-connect-http/scripts/control.sh launch
   ```

   `launch` starts four containers (`scripts/compose.yml`), waits until Connect's REST API and the targets answer, then creates topics `http-request`, `http-success`, `http-error`. It prints the run id and writes it to `artifacts/verify-kafka-connect-http/.current-run`.

   Ready means: `http://127.0.0.1:8083/connector-plugins` returns the connector classes **and** `http://127.0.0.1:8080/__admin/health` returns healthy. `launch` blocks until both are true; a process merely answering is not enough (Kafka's own healthcheck can pass while Connect is still starting).

   Servers, from the host and from inside the compose network:

   | service     | host address              | in-network address          |
   |-------------|---------------------------|-----------------------------|
   | Kafka       | `localhost:29092` (ext)   | `kafka:9092`                |
   | Connect REST| `http://127.0.0.1:8083`   | `http://connect:8083`       |
   | http-target | `http://127.0.0.1:8080`   | `http://http-target:8080`   |
   | sse-target  | `http://127.0.0.1:8082`   | `http://sse-target:8080`    |

   Connector configs use the **in-network** addresses, because the Connect worker reaches the target container-to-container. `http-target` is `wiremock/wiremock:3.13.2`; its `/__admin` API is both how you stub responses and how you read back the requests the connector made. `sse-target` is `jmalloc/echo-server:v0.3.7`, serving a perpetual SSE stream at `/.sse`.

Teardown is `control.sh cleanup` (see Cleanup).

If Docker is a snap-confined install and the repo lives under a hidden directory, as a `~/.herdr/` worktree does, `launch` fails with `permission denied` reading the compose file, because the snap cannot see hidden paths. Set `COMPOSE_FILE` to a byte-identical copy outside the hidden tree — and outside `/tmp` too, which the snap sees as its own private directory (a copy there still fails with `compose file ... is invalid: ... no such file or directory`; a copy in your home directory works). Everything else about the run is unchanged.

## Doctor

One read-only check that answers "is this instance worth driving?". Run it first whenever anything looks off:

```bash
.agents/skills/verify-kafka-connect-http/scripts/control.sh doctor
```

It confirms, without mutating anything: the four containers are running, the Connect REST API answers, all three connector classes (`HttpSinkConnector`, `CronSourceConnector`, `SseSourceConnector`) are in `GET /connector-plugins`, both targets are reachable, and host port 8083 is owned by *our* Connect container. It exits non-zero with a `FAIL` line per problem. It also dumps `/connector-plugins` to the run's `connector-plugins.json`.

If doctor fails because no run exists, run `launch`. If it fails on a port-ownership or plugin check, do not drive the instance — it is not the one this skill started.

## Drive

Every drive is: register a connector over the REST API, drive records, then read state back. `control.sh status` prints the current run id and host ports; the commands below resolve the run's compose project first so you can call `kafka-topics`, `kafka-console-producer` and `kafka-console-consumer` inside the Kafka container:

```bash
PROJECT="kchttp-$(cat artifacts/verify-kafka-connect-http/.current-run)"
COMPOSE=(docker compose -f .agents/skills/verify-kafka-connect-http/scripts/compose.yml -p "$PROJECT")
```

- **Register / update a connector.** PUT the config, then require the connector *and its task* to reach `RUNNING`:

  ```bash
  curl -sf -X PUT http://127.0.0.1:8083/connectors/<name>/config \
    -H 'Content-Type: application/json' -d @/tmp/<name>.json
  curl -sf http://127.0.0.1:8083/connectors/<name>/status | jq .
  ```

  With the default `StringConverter` worker converters, `"value.converter": "org.apache.kafka.connect.storage.StringConverter"` keeps the whole pipeline string-based (no Schema Registry needed).

- **Push a record into Kafka** (to drive the sink): produce from the Kafka container, which is on the same in-network address the connector uses.

  ```bash
  printf '%s\n' "$HTTP_REQUEST_JSON" | "${COMPOSE[@]}" exec -T kafka \
    kafka-console-producer --bootstrap-server localhost:9092 --topic http-request
  ```

- **Read records back out of Kafka** (to observe a source connector):

  ```bash
  "${COMPOSE[@]}" exec -T kafka kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic http-success --from-beginning --max-messages 1 --timeout-ms 30000
  ```

- **Read what the connector sent over HTTP** (to observe the sink):

  ```bash
  curl -sf http://127.0.0.1:8080/__admin/requests | jq .
  ```

Prefer stable handles: connector names you chose, topic names, the `/__admin/requests` log. Do not assert on timestamps, durations or generated ids.

`control.sh drive-sink` is the ready-made, end-to-end recipe for the HTTP Sink feature — run it to check the whole harness at once:

```bash
.agents/skills/verify-kafka-connect-http/scripts/control.sh drive-sink
```

## Evidence

Capture the **action** and the **resulting state**, plus the side effect, not just the last screen. For a sink drive that means: the config you registered, the connector/task state, the message you produced, and the HTTP request the target actually received (with method, absolute URL, headers, body) — the third is the real proof, the first two only prove the connector started.

`drive-sink` writes, into `artifacts/verify-kafka-connect-http/<RUN_ID>/`:

- `sink-config-response.json` — the `PUT /connectors/.../config` response
- `sink-config.json` — `GET /connectors/http-sink-verify/config`, what Connect stored
- `sink-status.json` — `GET /connectors/http-sink-verify/status`, connector + task state
- `produced-message.json` — the exact `HttpRequest` JSON put on `http-request`
- `requests.json` — the full `__admin/requests` log
- `received-request.json` — the single request matched on `POST /ping` and its body
- `connector-plugins.json` — `GET /connector-plugins`, proving plugin identity
- `SUMMARY.md` — the pass line with the received method/URL/body/status

Proof standards for this repo:

- Exercise the real user path (Connect REST API + real Kafka records), not internal setters, unit-level entry points or test-only endpoints.
- The sink proof is the request the target recorded, not the connector saying `RUNNING`.
- For source connectors, the proof is a record present in the destination Kafka topic, read with `kafka-console-consumer`; verify the record body, not only that the topic has one.
- The worker converters are `StringConverter` by default here, so no Schema Registry is needed. If a feature under test needs `JsonSchemaConverter`/Avro, you must add a Schema Registry to the stack — say so in the run report rather than silently changing the pipeline.
- The target is a stub (WireMock), which is a legitimate production boundary: it replaces the *external HTTP server* a user would call, not anything inside this connector.

The evidence directory is named by run id and lives outside the containers, so it survives `cleanup`. `cleanup` never deletes it.

## Cleanup

```bash
.agents/skills/verify-kafka-connect-http/scripts/control.sh cleanup
```

This runs `docker compose -p <project> down -v --remove-orphans` for this run's project only and deletes `artifacts/verify-kafka-connect-http/.current-run`. It never kills by process name and never touches the artifact directory, so proof survives. Run it after every drive, and after every *failed* attempt too, so broken runs do not strand containers and ports.

Do not use `control.sh cleanup` to remove somebody else's stack: with no `.current-run` file it refuses rather than guessing.

## Isolation

Two runs can coexist only if they use different host ports. The run id suffixes the compose project (`kchttp-<RUN_ID>`) so containers and networks never collide, but the default ports are fixed. To run a second stack beside the first, export ports before `launch`:

```bash
CONNECT_PORT=18083 TARGET_PORT=18080 SSE_PORT=18082 KAFKA_PORT=39092 RUN_ID=second \
  .agents/skills/verify-kafka-connect-http/scripts/control.sh launch
```

Otherwise there is one shared port set: **refuse to double-drive the same ports.** Never register connectors into a Connect worker this skill did not start — `doctor`'s port-ownership check exists for exactly that.

## Helpers

`scripts/control.sh` is the only helper and is executable. Its subcommands and the invocation of each are shown above and in `scripts/control.sh --help` / the header of that file:

| subcommand   | what it does                                            |
|--------------|---------------------------------------------------------|
| `build`      | Maven package -> jar + `clescot/kafka-connect-http:<ver>` image |
| `launch`     | start stack, wait for readiness, create topics          |
| `doctor`     | read-only readiness/identity check                      |
| `drive-sink` | drive the HTTP Sink feature end to end, capture evidence |
| `status`     | print run id, ports, artifact dir, container state      |
| `logs [svc]` | tail logs for `kafka`, `connect`, `http-target` or `sse-target`. A finite tail, so an agent can read evidence without blocking; add `-f` to follow |
| `cleanup`    | tear down the stack, keep evidence                      |
