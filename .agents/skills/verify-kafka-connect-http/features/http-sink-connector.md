# HTTP Sink connector

The HTTP Sink connector consumes `HttpRequest` JSON messages from a Kafka topic and performs the HTTP call each one describes, turning Kafka records into outbound HTTP traffic. It is the primary user-facing feature of this repo, and `control-kchttp drive-sink` runs this whole recipe.

## Sub-features

- `sink-register` registers the connector over the Connect REST API and reaches `RUNNING/RUNNING`.
- `sink-direct-request` turns a serialized `HttpRequest` message on the topic into the described HTTP method/URL/headers/body.
- `sink-jexl-mapping` a custom `JEXL`-mode request mapper builds the `HttpRequest` from expressions evaluated against the record.
- `sink-okhttp-identity` performs the call with the connector's own HTTP client (OkHttp), not with a tool run by the operator.
- `sink-not-published` with the default `publish.mode: NONE` the response is not republished to Kafka.

## How to get to it (user POV)

- Register the connector: `PUT http://127.0.0.1:8083/connectors/<name>/config` with `connector.class: io.github.clescot.kafka.connect.http.sink.HttpSinkConnector`.
- Point it at a topic with `topics`.
- Produce `HttpRequest` JSON messages onto that topic (any Kafka producer).

## Driving it with control-kchttp

Preconditions:

- A run is up and `control-kchttp doctor` passes.
- Topic `http-request` exists (created by `launch`).
- No connector named `http-sink-verify` is already registered.

The whole recipe is one command; the bullets below are what it does and what proves each step.

```bash
.agents/skills/verify-kafka-connect-http/scripts/control.sh drive-sink
```

- **Stub the target.** Register `POST /ping -> 200 {"result":"pong"}` on the target. Run `curl -sf -X POST http://127.0.0.1:8080/__admin/mappings -H 'Content-Type: application/json' -d '{"request":{"method":"POST","url":"/ping"},"response":{"status":200,"body":"{\"result\":\"pong\"}"}}'`. It returns the created mapping.
- **Register the connector.** PUT the config to `http://127.0.0.1:8083/connectors/http-sink-verify/config` with `connector.class: io.github.clescot.kafka.connect.http.sink.HttpSinkConnector`, `tasks.max: 1`, `topics: http-request`, `key.converter`/`value.converter: org.apache.kafka.connect.storage.StringConverter`, `publish.mode: NONE`. The response echoes the stored config.
- **Require RUNNING.** Poll `GET http://127.0.0.1:8083/connectors/http-sink-verify/status`. `connector.state` and `tasks[0].state` must both read `RUNNING`.
- **Produce one request.** Put this JSON on `http-request` (the driver substitutes its run id into `bodyAsString`, here shown as `20261025-192527`):
  ```json
  {"url":"http://http-target:8080/ping","method":"POST","headers":{"X-Request-ID":["verify-request-1"]},"bodyType":"STRING","bodyAsString":"kchttp-verify 20261025-192527"}
  ```
  Run `printf '%s\n' "$JSON" | "${COMPOSE[@]}" exec -T kafka kafka-console-producer --bootstrap-server localhost:9092 --topic http-request`. The producer exits 0.
- **Observe the side effect.** Poll `GET http://127.0.0.1:8080/__admin/requests`. A request appears whose `.request.absoluteUrl` is `http://http-target:8080/ping`, `.request.method` is `POST`, `.request.body` is the marker, and `.response.status` is `200`. This is the proof.
- **Proof.** `drive-sink` writes `received-request.json`, `sink-status.json`, `produced-message.json` and `SUMMARY.md` under `artifacts/verify-kafka-connect-http/<RUN_ID>/`. The pass line names the received method, URL, body and response status, and the driver only accepts a recorded request with `wasMatched: true`.

### JEXL mapping (`sink-jexl-mapping`)

`drive-sink` covers `DIRECT` only. To drive a JEXL-mode mapper, use a dedicated topic so the
`DIRECT` sink does not parse the same records:

```bash
PROJECT="kchttp-$(cat artifacts/verify-kafka-connect-http/.current-run)"
COMPOSE=(docker compose -f .agents/skills/verify-kafka-connect-http/scripts/compose.yml -p "$PROJECT")
"${COMPOSE[@]}" exec -T kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic http-request-jexl --partitions 1 --replication-factor 1
curl -sf -X PUT http://127.0.0.1:8083/connectors/http-sink-jexl-verify/config -H 'Content-Type: application/json' -d '{
  "connector.class": "io.github.clescot.kafka.connect.http.sink.HttpSinkConnector",
  "tasks.max": "1", "topics": "http-request-jexl",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.storage.StringConverter",
  "publish.mode": "NONE",
  "http.request.mapper.ids": "jx",
  "http.request.mapper.jx.mode": "JEXL",
  "http.request.mapper.jx.matcher": "sinkRecord.topic()==\u0027http-request-jexl\u0027",
  "http.request.mapper.jx.url": "\u0027http://http-target:8080/ping\u0027",
  "http.request.mapper.jx.method": "\u0027POST\u0027",
  "http.request.mapper.jx.body": "\u0027mapped-\u0027 + sinkRecord.value()"
}'
printf '%s\n' 'hello' | "${COMPOSE[@]}" exec -T kafka kafka-console-producer --bootstrap-server localhost:9092 --topic http-request-jexl
curl -sf http://127.0.0.1:8080/__admin/requests | jq '[.requests[]|select(.request.body=="mapped-hello" and .request.method=="POST" and .wasMatched)]|length'
```

- The last command must print `1`: the target recorded the request the expressions built.
- Verified on `20261029-210442`: `POST http://http-target:8080/ping` with body `mapped-hello`, plus
  a configured header, connector and task `RUNNING/RUNNING`. Evidence:
  `artifacts/verify-kafka-connect-http/20261029-210442/jexl/`.

## Gotchas

- The connector must reach the target by its **in-network** name. `http://127.0.0.1:8080/ping` in the message would make the Connect container call itself; use `http://http-target:8080/ping`.
- `url` is the only field you must provide: `method` defaults to `GET` and `bodyType` to `STRING` when omitted (fixed in #955). Set `method` explicitly whenever the stub expects anything but `GET`.
- `RUNNING` alone is not proof. Assert on the recorded request.
- The default `publish.mode` is `NONE`: nothing is republished to Kafka. In `PRODUCER` mode the published payload is the exchange, not the bare response — `producer.content` defaults to `exchange`.
- Keys are not sent to the target; only the message body is parsed into an `HttpRequest`.
- The target records requests even when no stub matches (a bare 404); a request found with `wasMatched: false` means your stub was wrong, not that the connector failed. Assert on `wasMatched` too.
- **`DIRECT` is the default.** A custom mapper (`http.request.mapper.ids`) only handles the records
  whose `matcher` is true; everything else falls through to the default mapper, which tries to parse
  the value as an `HttpRequest` and fails on a plain string. Give a custom mapper a matcher, or
  switch the default mapper to `JEXL` (`http.request.mapper.default.mode`).
- **JEXL mode requires `url`.** `http.request.mapper.<id>.url` is mandatory for a `JEXL` mapper;
  the other expressions default to `GET` and `STRING`. The raw value is not parsed as JSON.
- Re-running `drive-sink` PUTs the same connector name; WireMock accumulates requests, so match on the unique body marker (run id) rather than "any request".
