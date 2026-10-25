#!/usr/bin/env bash
#
# Run the SSE source-connector at-least-once check.
#
#   bash .agents/skills/verify-sse-source-no-event-loss/scripts/run-check.sh
#
# It regenerates the TLA+ facts from the Java source, model-checks the
# SseDelivery spec twice (two possible SSE server resume policies) and prints a
# verdict, plus a counterexample decoded into the connector's own steps in
# results/counterexample.txt.
#
set -uo pipefail

SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$SKILL_DIR/../../.." && pwd)"
SPEC_DIR="$SKILL_DIR/spec"
RESULTS_DIR="$SKILL_DIR/results"

# --- pinned model checker ---------------------------------------------------
TLA_VERSION="1.7.4"
TLA_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
TLA_URL="https://github.com/tlaplus/tlaplus/releases/download/v${TLA_VERSION}/tla2tools.jar"
TLA_CACHE="${XDG_CACHE_HOME:-$HOME/.cache}/kafka-connect-http-formal-verification"
# TLA2TOOLS_JAR points at a jar you already have, for a run with no network. Its
# sha256 still has to match the pinned version.
TLA_JAR="${TLA2TOOLS_JAR:-$TLA_CACHE/tla2tools-${TLA_VERSION}.jar}"

# --- work directories -------------------------------------------------------
TMP_BASE="${TMPDIR:-/tmp}"
[ -d /tmp/opencode ] && TMP_BASE="/tmp/opencode"
WORK="$(mktemp -d -p "$TMP_BASE" sse-no-event-loss.XXXXXX)"
mkdir -p "$RESULTS_DIR"
TLC_LOG="$RESULTS_DIR/tlc.txt"
TRACE_OUT="$RESULTS_DIR/counterexample.txt"

cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT

fail() { echo "run-check: $*" >&2; exit 4; }

# --- 1. model checker -------------------------------------------------------
if [ ! -f "$TLA_JAR" ]; then
  mkdir -p "$(dirname "$TLA_JAR")"
  echo "run-check: downloading tla2tools v${TLA_VERSION} (pinned) ..."
  curl -sfL -o "$TLA_JAR.tmp" "$TLA_URL" \
    || fail "cannot download $TLA_URL (offline? set TLA2TOOLS_JAR to a local copy of tla2tools v${TLA_VERSION})"
  mv "$TLA_JAR.tmp" "$TLA_JAR"
fi
ACTUAL_SHA="$(sha256sum "$TLA_JAR" | awk '{print $1}')"
if [ "$ACTUAL_SHA" != "$TLA_SHA256" ]; then
  fail "tla2tools.jar checksum mismatch: expected $TLA_SHA256, got $ACTUAL_SHA"
fi
JAVA_BIN="${JAVA_HOME:+$JAVA_HOME/bin/}java"
command -v "$JAVA_BIN" >/dev/null 2>&1 || fail "java not found (Java 11+ required by TLC)"

# --- 2. regenerate the facts from the real code -----------------------------
cp "$SPEC_DIR"/SseDelivery.tla "$SPEC_DIR"/*.cfg "$WORK"/
bash "$SKILL_DIR/scripts/extract-facts.sh" "$REPO_ROOT" "$WORK/Facts.tla" \
  || exit $?

# --- 3. model-check ---------------------------------------------------------
run_tlc() { # $1 = config name, $2 = log file
  local cfg="$1" log="$2"
  mkdir -p "$WORK/meta-$cfg"
  ( cd "$WORK" && "$JAVA_BIN" -XX:+UseParallelGC -cp "$TLA_JAR" tlc2.TLC \
      -metadir "$WORK/meta-$cfg" -config "$cfg.cfg" SseDelivery.tla ) > "$log" 2>&1
  if grep -q "Invariant NoGap is violated" "$log"; then echo "VIOLATED"; else
    if grep -q "No error has been found" "$log"; then echo "OK"; else echo "ERROR"; fi
  fi
}

FROM_START="$(run_tlc from_start "$RESULTS_DIR/from_start.tlc.txt")"
FROM_NOW="$(run_tlc from_now "$RESULTS_DIR/from_now.tlc.txt")"
FROM_NOW_RESTART="$(run_tlc from_now_restart "$RESULTS_DIR/from_now_restart.tlc.txt")"

{
  echo "== from_start =="; cat "$RESULTS_DIR/from_start.tlc.txt"
  echo; echo "== from_now =="; cat "$RESULTS_DIR/from_now.tlc.txt"
  echo; echo "== from_now_restart =="; cat "$RESULTS_DIR/from_now_restart.tlc.txt"
} > "$TLC_LOG"

# --- 4. explain a counterexample in the code's words ------------------------
decode_trace() { # $1 = tlc log
  local log="$1" n=0
  grep -oE '^State [0-9]+: <[A-Za-z]+' "$log" | sed -E 's/^State ([0-9]+): <([A-Za-z]+)/\1 \2/' |
  while read -r idx action; do
    case "$action" in
      Initial)     text="SseSourceTask.start(): task up, first SSE connection open" ;;
      Publish)     text="the SSE server publishes its next event (it keeps publishing while the task is down)" ;;
      Connect)     text="SseConfiguration.connect(): opens a fresh BackgroundEventSource at the resume point" ;;
      Receive)     text="SseBackgroundEventHandler.onMessage(): the event is put on the in-memory queue" ;;
      Drop)        text="the enqueue silently drops the event (lossy queue)" ;;
      Poll)        text="SseSourceTask.poll(): the whole queue is drained into SourceRecords" ;;
      Commit)      text="Kafka Connect commits the offsets of the returned records" ;;
      Disconnect)  text="the SSE connection drops; the library will reconnect" ;;
      Crash)       text="the worker loses the task: in-memory queue and uncommitted batch are gone" ;;
      Restart)     text="SseSourceTask.start(): a brand-new task instance comes up" ;;
      *)           text="$action" ;;
    esac
    echo "  $idx. $text"
  done
}

{
  echo "Finding F1 - two ways to trigger it"
  echo
  echo "F1a: a connection drops before the first event of the connection arrives"
  echo "     (AllowMidRunReconnect = TRUE):"
  decode_trace "$RESULTS_DIR/from_now.tlc.txt"
  echo
  echo "F1b: a task restart / rebalance loses the polled-but-uncommitted batch"
  echo "     (AllowMidRunReconnect = FALSE):"
  decode_trace "$RESULTS_DIR/from_now_restart.tlc.txt"
  if grep -q "Invariant NoGap is violated" "$RESULTS_DIR/from_start.tlc.txt"; then
    echo
    echo "from_start is ALSO violated (a break that is not F1) - counterexample:"
    decode_trace "$RESULTS_DIR/from_start.tlc.txt"
  fi
} > "$TRACE_OUT"

# --- 5. verdict -------------------------------------------------------------
echo
echo "SSE source at-least-once check - tla2tools v${TLA_VERSION} (pinned, sha256 verified)"
echo "  facts read from the code:"
grep -E '^(EnqueueLossless|SeedsFromCommittedOffset) ==' "$WORK/Facts.tla" | sed 's/^/    /'
echo "  from_start       : $FROM_START"
echo "  from_now         : $FROM_NOW"
echo "  from_now_restart : $FROM_NOW_RESTART"
echo "  results          : $RESULTS_DIR/ (trace decoded in counterexample.txt)"
echo

if [ "$FROM_START" = "VIOLATED" ]; then
  echo "VERDICT: FAIL - an event is lost even when the server replays from the"
  echo "         start of the stream. This is a NEW break in the guarded code,"
  echo "         not finding F1. See $RESULTS_DIR/from_start.tlc.txt."
  exit 1
fi

if [ "$FROM_START" != "OK" ]; then
  echo "VERDICT: ERROR - TLC could not run the from_start config (see $TLC_LOG)."
  exit 4
fi

if [ "$FROM_NOW" = "OK" ] && [ "$FROM_NOW_RESTART" = "OK" ]; then
  echo "VERDICT: F1 LOOKS FIXED - the connector no longer skips events when the"
  echo "         server resumes from 'now'; the resume cursor is seeded from the"
  echo "         committed offset. If the covered code changed since the sync"
  echo "         commit, follow 'Keeping this in step with the code' in SKILL.md."
  exit 3
fi

echo "VERDICT: BASELINE - from_start passes; the from_now configs reproduce"
echo "         finding F1 (the connector can skip events it never asked the"
echo "         server to replay). Counterexample in code words: $TRACE_OUT"
