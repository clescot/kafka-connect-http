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
`DIRECT` sink does not parse the same records; the mapper below therefore always matches. It builds
the URL, a header and the body from the record's event time, so all three date-time variables and
both `datetime` functions are exercised:

- `dateTime` in the URL (`?at=…`) and in the header;
- `timestamp` (epoch seconds) and `timestampInMs` (epoch millis) in the same header;
- `datetime:format(dateTime,'yyyy-MM-dd')` and
  `datetime:fromEpochMillis(timestampInMs,'yyyyMMdd')` in the body.

```bash
RUN_ID="$(cat artifacts/verify-kafka-connect-http/.current-run)"
ART="artifacts/verify-kafka-connect-http/$RUN_ID/jexl"
TOPIC="http-request-jexl-$RUN_ID"
mkdir -p "$ART"
PROJECT="kchttp-$RUN_ID"
COMPOSE=(docker compose -f .agents/skills/verify-kafka-connect-http/scripts/compose.yml -p "$PROJECT")
"${COMPOSE[@]}" exec -T kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic "$TOPIC" --partitions 1 --replication-factor 1

cat > "$ART/connector-config.json" <<JSON
{
  "connector.class": "io.github.clescot.kafka.connect.http.sink.HttpSinkConnector",
  "tasks.max": "1",
  "topics": "$TOPIC",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.storage.StringConverter",
  "publish.mode": "NONE",
  "http.request.mapper.ids": "jx",
  "http.request.mapper.jx.mode": "JEXL",
  "http.request.mapper.jx.matcher": "true",
  "http.request.mapper.jx.url": "'http://http-target:8080/ping?at=' + dateTime",
  "http.request.mapper.jx.method": "'POST'",
  "http.request.mapper.jx.headers": "{'X-Event-Time':[dateTime,'' + timestamp,'' + timestampInMs,...]}",
  "http.request.mapper.jx.body": "datetime:format(dateTime,'yyyy-MM-dd') + ' ' + datetime:fromEpochMillis(timestampInMs,'yyyyMMdd')"
}
JSON
curl -sf -X PUT http://127.0.0.1:8083/connectors/http-sink-jexl-verify/config \
  -H 'Content-Type: application/json' -d @"$ART/connector-config.json" > "$ART/config-response.json"

# require connector + task RUNNING/RUNNING before producing anything
for i in $(seq 1 60); do
  state="$(curl -sf http://127.0.0.1:8083/connectors/http-sink-jexl-verify/status | jq -r '.connector.state + "/" + (.tasks[0].state // "none")')"
  [ "$state" = "RUNNING/RUNNING" ] && break
  sleep 2
done
[ "$state" = "RUNNING/RUNNING" ]

# stub POST /ping (any query string) and start from an empty request log
curl -sf -X POST http://127.0.0.1:8080/__admin/mappings -H 'Content-Type: application/json' -d '{
  "request":  {"method": "POST", "urlPath": "/ping"},
  "response": {"status": 200, "headers": {"Content-Type": "application/json"}, "body": "{\"result\":\"pong\"}"}
}' > "$ART/mapping-response.json"
curl -sf -X DELETE http://127.0.0.1:8080/__admin/requests >/dev/null

# produce one record with a pinned timestamp: 1699228800000 ms = 2023-11-06T00:00:00.000Z.
# The Kafka console producer cannot set a record timestamp.
EVENT_MS=1699228800000
EVENT_SEC=$((EVENT_MS / 1000))
EVENT_ISO=2023-11-06T00:00:00.000Z
EVENT_BODY="2023-11-06 20231106"
.agents/skills/verify-kafka-connect-http/scripts/produce-pinned-timestamp.sh \
  localhost:29092 "$TOPIC" "$EVENT_MS" "jexl-verify $RUN_ID" | tee "$ART/producer.txt"

# one filter, reused by the wait loop and by the final assertion below
cat > "$ART/matched.jq" <<'JQ'
[.requests[]
 | select(.request.method == "POST")
 | select(.wasMatched == true)
 | select(.request.absoluteUrl | contains("/ping?at=" + $at))
 | select((.request.headers["X-Event-Time"] // []) | index($at) != null)
 | select((.request.headers["X-Event-Time"] // []) | index($sec) != null)
 | select((.request.headers["X-Event-Time"] // []) | index($ms) != null)
 | select(.request.body == $body)]
| length == 1
JQ

for i in $(seq 1 60); do
  curl -sf http://127.0.0.1:8080/__admin/requests -o "$ART/requests.json"
  jq -e -f "$ART/matched.jq" --arg at "$EVENT_ISO" --arg sec "$EVENT_SEC" \
    --arg ms "$EVENT_MS" --arg body "$EVENT_BODY" "$ART/requests.json" >/dev/null && break
  sleep 2
done
jq -e -f "$ART/matched.jq" --arg at "$EVENT_ISO" --arg sec "$EVENT_SEC" \
  --arg ms "$EVENT_MS" --arg body "$EVENT_BODY" "$ART/requests.json" >/dev/null
jq --arg at "$EVENT_ISO" --arg body "$EVENT_BODY" '
  [.requests[]
   | select(.request.method == "POST")
   | select(.request.absoluteUrl | contains("/ping?at=" + $at))
   | select(.request.body == $body)]
  | first' "$ART/requests.json" > "$ART/received-request.json"
curl -sf http://127.0.0.1:8083/connectors/http-sink-jexl-verify/status | jq . > "$ART/status.json"
curl -sf http://127.0.0.1:8083/connectors/http-sink-jexl-verify/config | jq . > "$ART/config.json"
```

- The `jq -e` loop must succeed: exactly one recorded, matched `POST` has
  `/ping?at=2023-11-06T00:00:00.000Z`, header `X-Event-Time` values
  `2023-11-06T00:00:00.000Z`, `1699228800` and `1699228800000`, and body
  `2023-11-06 20231106`. Those values are the pinned event time, not the wall clock.
- Verified on `20261029-225830`: `POST http://http-target:8080/ping?at=2023-11-06T00:00:00.000Z`
  with header `X-Event-Time: [2023-11-06T00:00:00.000Z,1699228800,1699228800000]` and body
  `2023-11-06 20231106`; connector and task `RUNNING/RUNNING`. Evidence:
  `artifacts/verify-kafka-connect-http/20261029-225830/jexl/`.

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
- **`sinkRecord` method calls did not resolve in the `20261029-225830` stack.** A matcher using
  `sinkRecord.topic()` evaluated false and a body using `sinkRecord.value()` failed with a null
  operand, while the top-level `timestamp`/`timestampInMs`/`dateTime` variables worked. The recipe
  therefore matches every record on its dedicated topic; investigate the mapper/sandbox before
  relying on `sinkRecord` method calls in a live sink.
- **JEXL mode requires `url`.** `http.request.mapper.<id>.url` is mandatory for a `JEXL` mapper;
  the other expressions default to `GET` and `STRING`. The raw value is not parsed as JSON.
- **A JEXL list literal needs the trailing `...`.** `{'X':[a,b,...]}` evaluates to a `List`; without
  the `...` JEXL returns a raw array and the sink fails casting it to `List<String>`.
- Re-running `drive-sink` PUTs the same connector name; WireMock accumulates requests, so match on the unique body marker (run id) rather than "any request".
