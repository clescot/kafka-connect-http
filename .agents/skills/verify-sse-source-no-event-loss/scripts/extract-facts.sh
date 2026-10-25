#!/usr/bin/env bash
#
# Extract the two "facts" that SseDelivery.tla is parameterised by, from the
# real Java source of the SSE source connector, and write them as a TLA+
# module (Facts.tla).
#
# This script is the bridge between the code and the model. It is deliberately
# small and strict: it fails loudly when it can no longer find the code it
# reads, so that a structural change never silently leaves the model behind.
#
# usage: extract-facts.sh <repo-root> <output-Facts.tla>
#
set -euo pipefail

REPO_ROOT="${1:?usage: extract-facts.sh <repo-root> <output-file>}"
OUT="${2:?usage: extract-facts.sh <repo-root> <output-file>}"

QUEUE_FACTORY="$REPO_ROOT/http-client/src/main/java/io/github/clescot/client/queue/QueueFactory.java"
SSE_DIR="$REPO_ROOT/kafka-connect-http-connectors/src/main/java/io/github/clescot/kafka/connect/sse/client/okhttp"

HANDLER="$SSE_DIR/SseBackgroundEventHandler.java"
SOURCE_TASK="$SSE_DIR/SseSourceTask.java"
SSE_TASK="$SSE_DIR/SseTask.java"
SSE_CONFIGURATION="$SSE_DIR/SseConfiguration.java"

for f in "$QUEUE_FACTORY" "$HANDLER" "$SOURCE_TASK" "$SSE_TASK" "$SSE_CONFIGURATION"; do
  if [ ! -f "$f" ]; then
    echo "extract-facts: expected source file not found: $f" >&2
    echo "extract-facts: the SSE delivery code has moved; update scripts/extract-facts.sh" >&2
    exit 2
  fi
done

# ---------------------------------------------------------------------------
# fact 1: enqueue-is-lossless
#
#   TRUE  iff the in-memory queue that receives SSE events cannot silently
#         drop an event.
#
#   The events reach the queue through SseBackgroundEventHandler.onMessage(),
#   which must still perform an explicit enqueue. QueueFactory.getQueue()
#   must still build an unbounded queue (ConcurrentLinkedQueue, or a
#   LinkedBlockingQueue with no capacity). A bounded queue (ArrayBlockingQueue,
#   or LinkedBlockingQueue with a capacity) can drop silently when the
#   producer outruns poll().
# ---------------------------------------------------------------------------

# the enqueue site must still exist, whatever it is called
if ! grep -qE 'queue[[:space:]]*\.(add|offer|put|addAll)[[:space:]]*\(' "$HANDLER"; then
  echo "extract-facts: no queue enqueue call found in $HANDLER" >&2
  echo "extract-facts: SseBackgroundEventHandler.onMessage no longer puts events on the queue" >&2
  exit 3
fi

# find the queue constructor used by QueueFactory.getQueue(), tolerating a
# fully-qualified class name and any generic argument list.
QUEUE_CTOR="$(grep -oE 'new[[:space:]]+[A-Za-z_][A-Za-z0-9_.]*[[:space:]]*<[^>]*>[[:space:]]*\([^)]*\)' "$QUEUE_FACTORY" | head -1)"
if [ -z "$QUEUE_CTOR" ]; then
  echo "extract-facts: no queue constructor found in $QUEUE_FACTORY" >&2
  exit 4
fi
QUEUE_CLASS="$(echo "$QUEUE_CTOR" | sed -E 's/.*new[[:space:]]+([A-Za-z_][A-Za-z0-9_.]*).*/\1/')"
QUEUE_BASE="${QUEUE_CLASS##*.}"
QUEUE_ARGS="$(echo "$QUEUE_CTOR" | sed -E 's/^[^(]*\(([^)]*)\).*$/\1/' | tr -d '[:space:]')"

case "$QUEUE_BASE" in
  ConcurrentLinkedQueue)
    QUEUE_KIND="$QUEUE_BASE (unbounded)"
    ENQUEUE_LOSSLESS=TRUE ;;
  LinkedBlockingQueue)
    if [ -z "$QUEUE_ARGS" ]; then
      QUEUE_KIND="$QUEUE_BASE() (unbounded)"
      ENQUEUE_LOSSLESS=TRUE
    else
      QUEUE_KIND="$QUEUE_BASE($QUEUE_ARGS) (bounded)"
      ENQUEUE_LOSSLESS=FALSE
    fi ;;
  ArrayBlockingQueue|LinkedBlockingDeque|ArrayDeque|ConcurrentLinkedDeque)
    if [ -z "$QUEUE_ARGS" ]; then
      QUEUE_KIND="$QUEUE_BASE()"
      ENQUEUE_LOSSLESS=TRUE
    else
      QUEUE_KIND="$QUEUE_BASE($QUEUE_ARGS) (bounded)"
      ENQUEUE_LOSSLESS=FALSE
    fi ;;
  *)
    echo "extract-facts: cannot classify queue type '$QUEUE_BASE' ($QUEUE_CTOR)" >&2
    echo "extract-facts: add its capacity to scripts/extract-facts.sh, then rerun" >&2
    exit 4 ;;
esac

# ---------------------------------------------------------------------------
# fact 2: seeds-from-committed-offset
#
#   TRUE  iff the SSE delivery code calls something that seeds the resume cursor
#         from what Kafka Connect committed: offsetStorageReader(), the
#         eventsource builder's lastEventId(), or an explicit Last-Event-ID
#         request header. This is what makes a restart or a rebalance replay
#         events that were received but not yet committed.
#
#   It is a call-site check, so it proves the plumbing exists, not that the value
#   fed in is the right one. A seeded wrong value still passes; only a live run
#   against a server that honours the header can tell.
# ---------------------------------------------------------------------------

# Match a call, not prose. An earlier version matched the bare words, so a comment
# or javadoc that merely named Last-Event-ID kept the fact TRUE after the seeding
# itself was removed, and the check passed on broken code.
SEEDS_CALL_PATTERN='\.offsetStorageReader[[:space:]]*\(|\.lastEventId[[:space:]]*\(|"Last-Event-ID"'
if grep -rqE "$SEEDS_CALL_PATTERN" "$SOURCE_TASK" "$SSE_TASK" "$SSE_CONFIGURATION"; then
  SEEDS_FROM_OFFSET=TRUE
else
  SEEDS_FROM_OFFSET=FALSE
fi

cat > "$OUT" <<EOF
-------------------------------- MODULE Facts ---------------------------------
(***************************************************************************)
(* GENERATED by scripts/extract-facts.sh. Do not edit by hand.              *)
(*                                                                          *)
(* Two facts about the real SSE delivery code, read straight out of the     *)
(* Java source. Regenerate with scripts/run-check.sh.                       *)
(***************************************************************************)

\* fact 1: enqueue-is-lossless
\* queue implementation found: $QUEUE_KIND
\* queue built in: http-client/src/main/java/io/github/clescot/client/queue/QueueFactory.java
\* enqueue site  : kafka-connect-http-connectors/src/main/java/io/github/clescot/kafka/connect/sse/client/okhttp/SseBackgroundEventHandler.java
EnqueueLossless == $ENQUEUE_LOSSLESS

\* fact 2: seeds-from-committed-offset
\* checked files: SseSourceTask.java, SseTask.java, SseConfiguration.java
SeedsFromCommittedOffset == $SEEDS_FROM_OFFSET

===============================================================================
EOF

echo "extract-facts: EnqueueLossless=$ENQUEUE_LOSSLESS ($QUEUE_KIND)"
echo "extract-facts: SeedsFromCommittedOffset=$SEEDS_FROM_OFFSET"
