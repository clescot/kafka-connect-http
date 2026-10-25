#!/usr/bin/env bash
#
# control-kchttp: the single entry point for driving kafka-connect-http.
# Run everything from anywhere; the script resolves the repo root itself.
#
#   control.sh build          build the connector jar + docker image (Maven package)
#   control.sh launch         start kafka + connect + http-target; create topics
#   control.sh doctor         read-only health/identity check (run this when unsure)
#   control.sh drive-sink     drive the HTTP Sink feature end to end + capture evidence
#   control.sh drive-ratelimit drive the server-advertised RateLimit feature end to end + capture evidence
#   control.sh status         show the current run id, ports and container state
#   control.sh logs [svc] [-f] tail logs, finite by default (-f to follow)
#                             (svc: kafka|connect|http-target|sse-target)
#   control.sh cleanup        remove the stack and scratch state (evidence survives)
#
# Environment overrides:
#   RUN_ID        reuse a specific run id instead of the timestamped default
#   COMPOSE_FILE  compose file to use (default: this skill's scripts/compose.yml).
#                 Point it at a byte-identical copy outside any hidden directory
#                 when Docker is snap-confined and the repo lives under one, such
#                 as ~/.herdr/: the snap cannot read hidden paths and 'launch'
#                 fails with "permission denied" on the compose file.
#   CONNECT_PORT  host port for the Connect REST API (default 8083)
#   TARGET_PORT   host port for the http-target WireMock admin API (default 8080)
#   SSE_PORT      host port for the sse-target echo server (default 8082)
#   KAFKA_PORT    host port for the external Kafka listener (default 29092)
#   KCHTTP_IMAGE  override the connector image (default derived from pom.xml version)
set -euo pipefail

SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$SKILL_DIR/scripts/compose.yml}"
REPO_ROOT="$(git -C "$SKILL_DIR" rev-parse --show-toplevel)"
STATE_DIR="$REPO_ROOT/artifacts/verify-kafka-connect-http"
CURRENT_RUN_FILE="$STATE_DIR/.current-run"
COMPOSE_BIN="${COMPOSE_BIN:-docker compose}"

log()  { printf '[control-kchttp] %s\n' "$*" >&2; }
die()  { printf '[control-kchttp] ERROR: %s\n' "$*" >&2; exit 1; }

pom_version() {
  sed -n '0,/<version>[^<]*<\/version>/s/.*<version>\([^<]*\)<\/version>.*/\1/p' \
    "$REPO_ROOT/pom.xml" | head -1
}

# --- run identity -------------------------------------------------------------

new_run_id() { date +%Y%m%d-%H%M%S; }

set_run_id() {
  RUN_ID="${RUN_ID:-$(new_run_id)}"
  COMPOSE_PROJECT="kchttp-$RUN_ID"
  ART="$STATE_DIR/$RUN_ID"
  export RUN_ID COMPOSE_PROJECT
  export CONNECT_PORT="${CONNECT_PORT:-8083}"
  export TARGET_PORT="${TARGET_PORT:-8080}"
  export KAFKA_PORT="${KAFKA_PORT:-29092}"
  export SSE_PORT="${SSE_PORT:-8082}"
  export KCHTTP_IMAGE="${KCHTTP_IMAGE:-clescot/kafka-connect-http:$(pom_version)}"
  CONNECT_URL="http://127.0.0.1:$CONNECT_PORT"
  TARGET_URL="http://127.0.0.1:$TARGET_PORT"
  SSE_URL="http://127.0.0.1:$SSE_PORT"
}

load_run_id() {
  [ -f "$CURRENT_RUN_FILE" ] \
    || die "no current run; run 'control.sh launch' first (looked for $CURRENT_RUN_FILE)"
  RUN_ID="$(cat "$CURRENT_RUN_FILE")"
  set_run_id
}

dc() { $COMPOSE_BIN -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" "$@"; }

# --- subcommands --------------------------------------------------------------

cmd_build() {
  [ -f "$REPO_ROOT/pom.xml" ] || die "no pom.xml at $REPO_ROOT"
  log "building connector jar + docker image (this compiles http-core, http-client, connectors)"
  ( cd "$REPO_ROOT" && mvn -B -q -pl kafka-connect-http-connectors -am clean package -DskipTests )
  log "image built: clescot/kafka-connect-http:$(pom_version)"
}

wait_for_http() {
  local url="$1" label="$2" tries="${3:-60}"
  local i=0
  while [ "$i" -lt "$tries" ]; do
    if curl -sf -o /dev/null "$url"; then
      log "ready: $label ($url)"
      return 0
    fi
    i=$((i + 1))
    sleep 2
  done
  return 1
}

cmd_launch() {
  set_run_id
  mkdir -p "$ART"
  echo "$RUN_ID" > "$CURRENT_RUN_FILE"
  log "run id: $RUN_ID (project $COMPOSE_PROJECT, artifacts $ART)"
  log "image:  $KCHTTP_IMAGE"

  docker image inspect "$KCHTTP_IMAGE" >/dev/null 2>&1 \
    || die "image $KCHTTP_IMAGE not found; run 'control.sh build' first"

  log "starting stack"
  dc up -d

  wait_for_http "$CONNECT_URL/connector-plugins" "kafka-connect REST API" 90 \
    || die "Connect REST API never became ready; inspect with 'control.sh logs connect'"
  wait_for_http "$TARGET_URL/__admin/health" "http-target (WireMock)" 60 \
    || die "http-target never became ready; inspect with 'control.sh logs http-target'"
  wait_for_http "$SSE_URL/" "sse-target (echo-server)" 30 \
    || die "sse-target never became ready; inspect with 'control.sh logs sse-target'"

  log "creating topics"
  local topic
  for topic in http-request http-success http-error; do
    dc exec -T kafka kafka-topics --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1 >/dev/null
  done
  log "launch complete; verify with 'control.sh doctor'"
}

cmd_doctor() {
  load_run_id
  local failures=0
  log "doctor for run $RUN_ID (compose project $COMPOSE_PROJECT)"
  log "  connect:     $CONNECT_URL"
  log "  http-target: $TARGET_URL"
  log "  sse-target:  $SSE_URL"
  log "  image:       $KCHTTP_IMAGE"

  local svc id running
  for svc in kafka connect http-target sse-target; do
    id="$(dc ps -q "$svc" 2>/dev/null || true)"
    if [ -z "$id" ]; then
      log "FAIL container '$svc' is not created"; failures=$((failures + 1)); continue
    fi
    running="$(docker inspect -f '{{.State.Running}}' "$id")"
    if [ "$running" = "true" ]; then
      log "ok   container '$svc' running ($id)"
    else
      log "FAIL container '$svc' not running ($id)"; failures=$((failures + 1))
    fi
  done

  if curl -sf "$CONNECT_URL/connector-plugins" -o "$ART/connector-plugins.json" 2>/dev/null; then
    log "ok   Connect REST API answers /connector-plugins"
    local cls
    for cls in \
      io.github.clescot.kafka.connect.http.sink.HttpSinkConnector \
      io.github.clescot.kafka.connect.http.source.cron.CronSourceConnector \
      io.github.clescot.kafka.connect.sse.client.okhttp.SseSourceConnector
    do
      if jq -e --arg c "$cls" 'any(.[]; .class == $c)' "$ART/connector-plugins.json" >/dev/null 2>&1; then
        log "ok   plugin installed: $cls"
      else
        log "FAIL plugin missing from /connector-plugins: $cls"; failures=$((failures + 1))
      fi
    done
  else
    log "FAIL Connect REST API unreachable at $CONNECT_URL/connector-plugins"
    failures=$((failures + 1))
  fi

  if curl -sf "$TARGET_URL/__admin/health" -o /dev/null 2>/dev/null; then
    log "ok   http-target admin API answers /__admin/health"
  else
    log "FAIL http-target admin API unreachable at $TARGET_URL/__admin/health"
    failures=$((failures + 1))
  fi

  if curl -sf "$SSE_URL/" -o /dev/null 2>/dev/null; then
    log "ok   sse-target answers at $SSE_URL/ (stream at $SSE_URL/.sse)"
  else
    log "FAIL sse-target unreachable at $SSE_URL/"
    failures=$((failures + 1))
  fi

  # own the port: refuse to drive a Connect we did not start
  local owner
  owner="$(dc ps -q connect 2>/dev/null || true)"
  if [ -n "$owner" ] && docker port "$owner" 8083/tcp 2>/dev/null | grep -q ":$CONNECT_PORT$"; then
    log "ok   port $CONNECT_PORT is owned by our connect container"
  else
    log "FAIL port $CONNECT_PORT is not owned by our connect container"
    failures=$((failures + 1))
  fi

  if [ "$failures" -eq 0 ]; then
    log "doctor: PASS"
  else
    log "doctor: FAIL ($failures problem(s))"
    return 1
  fi
}

cmd_drive_sink() {
  load_run_id
  [ -d "$ART" ] || die "artifact dir missing: $ART"
  local marker="kchttp-verify $RUN_ID"
  local payload
  payload="$(jq -nc --arg url "http://http-target:8080/ping" --arg body "$marker" \
    '{url:$url, method:"POST", headers:{"X-Request-ID":["verify-request-1"]}, bodyType:"STRING", bodyAsString:$body}')"

  log "step 1/6 register the target expectation (POST /ping -> 200 pong)"
  curl -sf -X POST "$TARGET_URL/__admin/mappings" -H 'Content-Type: application/json' -d '{
    "request":  {"method": "POST", "url": "/ping"},
    "response": {"status": 200, "headers": {"Content-Type": "application/json"}, "body": "{\"result\":\"pong\"}"}
  }' >/dev/null

  log "step 2/6 register the HTTP Sink connector via the Connect REST API"
  curl -sf -X PUT "$CONNECT_URL/connectors/http-sink-verify/config" \
    -H 'Content-Type: application/json' -d '{
      "connector.class": "io.github.clescot.kafka.connect.http.sink.HttpSinkConnector",
      "tasks.max": "1",
      "topics": "http-request",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "org.apache.kafka.connect.storage.StringConverter",
      "publish.mode": "NONE"
    }' > "$ART/sink-config-response.json"

  log "step 3/6 wait for connector + task to be RUNNING"
  local i=0 state=""
  while [ "$i" -lt 60 ]; do
    state="$(curl -sf "$CONNECT_URL/connectors/http-sink-verify/status" \
      | jq -r '.connector.state + "/" + (.tasks[0].state // "none")' 2>/dev/null || echo "")"
    [ "$state" = "RUNNING/RUNNING" ] && break
    i=$((i + 1)); sleep 2
  done
  [ "$state" = "RUNNING/RUNNING" ] || die "sink connector never reached RUNNING/RUNNING (last: $state)"

  log "step 4/6 produce one HttpRequest message onto topic http-request"
  printf '%s\n' "$payload" | dc exec -T kafka kafka-console-producer \
    --bootstrap-server localhost:9092 --topic http-request
  printf '%s\n' "$payload" > "$ART/produced-message.json"

  log "step 5/6 wait for the target to record the request"
  local matched=""
  i=0
  while [ "$i" -lt 60 ]; do
    if curl -sf "$TARGET_URL/__admin/requests" -o "$ART/requests.json"; then
      matched="$(jq -c --arg b "$marker" \
        '[.requests[] | select(.request.absoluteUrl | endswith("/ping")) | select(.request.body == $b) | select(.wasMatched == true)] | first' \
        "$ART/requests.json" 2>/dev/null || echo "")"
      [ -n "$matched" ] && [ "$matched" != "null" ] && break
    fi
    i=$((i + 1)); sleep 2
  done
  [ -n "$matched" ] && [ "$matched" != "null" ] \
    || die "target never received POST /ping with body '$marker'"

  log "step 6/6 capture evidence"
  printf '%s\n' "$matched" | jq . > "$ART/received-request.json"
  curl -sf "$CONNECT_URL/connectors/http-sink-verify/config" | jq . > "$ART/sink-config.json"
  curl -sf "$CONNECT_URL/connectors/http-sink-verify/status" | jq . > "$ART/sink-status.json"
  curl -sf "$CONNECT_URL/connector-plugins" | jq . > "$ART/connector-plugins.json"

  local url method body status
  url="$(jq -r '.request.absoluteUrl' "$ART/received-request.json")"
  method="$(jq -r '.request.method' "$ART/received-request.json")"
  body="$(jq -r '.request.body' "$ART/received-request.json")"
  status="$(jq -r '.responseDefinition.status' "$ART/received-request.json")"

  cat > "$ART/SUMMARY.md" <<EOF
# HTTP Sink verification — PASS

- run id:           $RUN_ID
- connector:        http-sink-verify (io.github.clescot.kafka.connect.http.sink.HttpSinkConnector)
- connector state:  RUNNING/RUNNING
- produced to:      http-request
- target received:  $method $url
- request body:     $body
- target responded: HTTP $status

## Artifacts
- \`sink-config-response.json\` — PUT /connectors/http-sink-verify/config response
- \`sink-config.json\`          — GET  /connectors/http-sink-verify/config (what Connect stored)
- \`sink-status.json\`          — GET  /connectors/http-sink-verify/status (connector + task state)
- \`produced-message.json\`     — the HttpRequest message put on the topic
- \`requests.json\`             — full WireMock __admin/requests log
- \`received-request.json\`     — the single request matched by /ping + body
- \`connector-plugins.json\`    — GET /connector-plugins (plugin identity)
EOF

  log "PROOF PASS: target received $method $url with body '$body', responded HTTP $status"
  log "evidence: $ART"
}

cmd_drive_ratelimit() {
  load_run_id
  [ -d "$ART" ] || die "artifact dir missing: $ART"
  local marker="ratelimit-verify $RUN_ID $(date +%H%M%S%N)"
  local window=5
  local topic=ratelimit-request

  log "step 1/7 create the dedicated topic '$topic' (other sink recipes leave consumers on http-request)"
  dc exec -T kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1 >/dev/null

  log "step 2/7 register the target expectation (POST /limited -> 200 with an exhausted RateLimit header)"
  curl -sf -X POST "$TARGET_URL/__admin/mappings" -H 'Content-Type: application/json' -d '{
    "request":  {"method": "POST", "url": "/limited"},
    "response": {"status": 200, "headers": {"Content-Type": "text/plain", "RateLimit": "\"default\";r=0;t=5"}, "body": "limited-ok"}
  }' > "$ART/ratelimit-mapping-response.json"

  log "step 3/7 register the HTTP Sink connector via the Connect REST API"
  jq -nc --arg topic "$topic" '{
      "connector.class": "io.github.clescot.kafka.connect.http.sink.HttpSinkConnector",
      "tasks.max": "1",
      "topics": $topic,
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "org.apache.kafka.connect.storage.StringConverter",
      "publish.mode": "NONE"
    }' > "$ART/ratelimit-config.json"
  curl -sf -X PUT "$CONNECT_URL/connectors/http-sink-ratelimit/config" \
    -H 'Content-Type: application/json' -d @"$ART/ratelimit-config.json" > "$ART/ratelimit-config-response.json"

  log "step 4/7 wait for connector + task to be RUNNING"
  local i=0 state=""
  while [ "$i" -lt 60 ]; do
    state="$(curl -sf "$CONNECT_URL/connectors/http-sink-ratelimit/status" \
      | jq -r '.connector.state + "/" + (.tasks[0].state // "none")' 2>/dev/null || echo "")"
    [ "$state" = "RUNNING/RUNNING" ] && break
    i=$((i + 1)); sleep 2
  done
  [ "$state" = "RUNNING/RUNNING" ] || die "sink connector never reached RUNNING/RUNNING (last: $state)"

  log "step 5/7 produce the three requests one at a time, each as soon as the previous was recorded"
  log "        (the producer adds no spacing of its own; any gap is the connector's pause)"
  local n payload seen
  # WireMock's request log carries no timestamps, so each request is timestamped by the first
  # poll that sees it.
  local -a recorded_at=()
  for n in 1 2 3; do
    payload="$(jq -nc --arg url "http://http-target:8080/limited" --arg body "$marker #$n" \
      '{url:$url, method:"POST", bodyType:"STRING", bodyAsString:$body}')"
    printf '%s\n' "$payload" | dc exec -T kafka kafka-console-producer \
      --bootstrap-server localhost:9092 --topic "$topic"
    seen=""
    i=0
    while [ "$i" -lt 240 ]; do
      if curl -sf "$TARGET_URL/__admin/requests" -o "$ART/ratelimit-requests.json"; then
        if jq -e --arg m "$marker #$n" \
          '[.requests[] | select(.request.body == $m)] | length > 0' \
          "$ART/ratelimit-requests.json" >/dev/null 2>&1; then
          seen="$(date +%s%3N)"
          break
        fi
      fi
      i=$((i + 1)); sleep 0.25
    done
    [ -n "$seen" ] || die "request #$n was never recorded"
    recorded_at+=("$seen")
    log "request #$n recorded"
  done

  log "step 6/7 capture evidence (the gaps between first sightings are the pause)"
  printf '%s\n' "${recorded_at[@]}" > "$ART/ratelimit-dates.txt"
  local gaps min_gap
  gaps="$(awk 'NR>1 {print $1 - previous} {previous=$1}' "$ART/ratelimit-dates.txt")"
  printf '%s\n' "$gaps" > "$ART/ratelimit-gaps-ms.txt"
  min_gap="$(printf '%s\n' "$gaps" | sort -n | head -1)"
  [ -n "$min_gap" ] || die "could not compute the gaps between the recorded requests"
  [ "$min_gap" -ge 4000 ] || die "requests were not paced: smallest gap ${min_gap}ms for an advertised window of ${window}s"
  dc logs --tail=400 connect > "$ART/ratelimit-connect-logs.txt" 2>&1 || true

  log "step 7/7 write the summary"
  cat > "$ART/ratelimit-SUMMARY.md" <<EOF
# RateLimit headers verification — PASS

- run id:            $RUN_ID
- connector:         http-sink-ratelimit (io.github.clescot.kafka.connect.http.sink.HttpSinkConnector)
- connector state:   RUNNING/RUNNING
- produced to:       $topic (3 requests, marker '$marker')
- advertised:        RateLimit: "default";r=0;t=${window}
- producer cadence:  the next request is produced as soon as the previous one was recorded
- recorded gaps(ms): $(printf '%s' "$gaps" | tr '\n' ' ')
- smallest gap(ms):  $min_gap

## Artifacts
- \`ratelimit-config.json\`          — the exact config PUT to Connect
- \`ratelimit-config-response.json\` — PUT /connectors/http-sink-ratelimit/config response
- \`ratelimit-requests.json\`        — full WireMock __admin/requests log
- \`ratelimit-dates.txt\`            — first-sighting epoch ms of each request, oldest first
- \`ratelimit-gaps-ms.txt\`          — gaps between consecutive first sightings
- \`ratelimit-connect-logs.txt\`     — worker log tail (shows the "waiting ..." lines)
EOF

  log "PROOF PASS: three requests recorded, smallest gap ${min_gap}ms for an advertised ${window}s window"
  log "evidence: $ART"
}

cmd_status() {
  load_run_id
  log "run id:      $RUN_ID"
  log "artifacts:   $ART"
  log "image:       $KCHTTP_IMAGE"
  log "connect:     $CONNECT_URL   (host port $CONNECT_PORT)"
  log "http-target: $TARGET_URL   (host port $TARGET_PORT)"
  log "sse-target:  $SSE_URL   (host port $SSE_PORT)"
  log "kafka ext:   localhost:$KAFKA_PORT"
  dc ps
}

cmd_logs() {
  load_run_id
  local svc="${1:-connect}"
  local follow=""
  case "${2:-}" in -f | --follow) follow="-f" ;; esac
  # A finite tail by default: an agent reading logs for evidence must not block.
  dc logs --tail="${KCHTTP_LOG_TAIL:-200}" $follow "$svc"
}

cmd_cleanup() {
  load_run_id
  log "removing stack for run $RUN_ID (project $COMPOSE_PROJECT)"
  dc down -v --remove-orphans >/dev/null 2>&1 || true
  rm -f "$CURRENT_RUN_FILE"
  log "cleaned up; evidence preserved at $ART"
}

usage() {
  awk 'NR>1 && /^#/ {sub(/^# ?/, ""); print; next} NR>1 {exit}' "${BASH_SOURCE[0]}"
}

main() {
  local cmd="${1:-}"
  case "$cmd" in
    build)      cmd_build ;;
    launch)     cmd_launch ;;
    doctor)     cmd_doctor ;;
    drive-sink) cmd_drive_sink ;;
    drive-ratelimit) cmd_drive_ratelimit ;;
    status)     cmd_status ;;
    logs)       shift; cmd_logs "$@" ;;
    cleanup)    cmd_cleanup ;;
    ""|-h|--help|help) usage ;;
    *) die "unknown command '$cmd'" ;;
  esac
}

main "$@"
