---
name: verify-sse-source-no-event-loss
description: "Model-check that the SSE source connector never skips an SSE event (at-least-once delivery). Use when a change touches the SSE source task, the SSE connection/resume logic, the in-memory event queue, the poll drain loop, or the source offsets, or when asked to verify at-least-once / no-event-loss for the SSE source."
---

# Verify: the SSE source never skips an event

**Property (one sentence).** No SSE event that the server publishes is ever skipped: if a
later event reaches Kafka, every earlier published event has reached Kafka too.

**Harm (one sentence).** A skipped event is silent data loss — the connector commits a later
offset and the missed event can never be recovered.

The check is a TLA+ model of the SSE delivery pipeline, run with TLC. It explores every order
of connect / receive / poll / commit / disconnect / crash / restart up to a bound, instead of
the handful of orders the unit and integration tests happen to exercise.

## Run it

```bash
bash .agents/skills/verify-sse-source-no-event-loss/scripts/run-check.sh
```

One command. It downloads the pinned model checker if needed (tla2tools v1.7.4, sha256
verified, or `TLA2TOOLS_JAR` pointing at a local copy of that version), re-reads the facts from
the Java source, runs TLC three times, writes the raw
results under `.agents/skills/verify-sse-source-no-event-loss/results/`, and prints a verdict.
Reads and writes only the paths above; it never changes product code.

**Read `results/counterexample.txt`, not the raw TLC trace.** The script decodes each violation
into the connector's own steps (`SseSourceTask.start`, `poll`, `SseConfiguration.connect`, …), so
you never have to parse TLC output by hand. If your harness has the pstack `/statechart` skill it
will also show the shortest path in plain words and which violations a change fixes; use it when
it is available, and read `results/counterexample.txt` when it is not.

## Does your change touch the verified code?

The check covers these files and methods — a change to any of them needs this check:

| File (all under `kafka-connect-http-connectors/src/main/java/io/github/clescot/kafka/connect/sse/client/okhttp/`) | Method / part |
| --- | --- |
| `SseSourceTask.java` | `start`, `poll`, `stop`, and the `{configurationId}` source partition it exports |
| `SseTask.java` | `connect`, `start` |
| `SseConfiguration.java` | `connect` (the `BackgroundEventSource` builder, retry/error strategy, `ConnectStrategy`, and the `lastEventId` seed) |
| `SseResumeLookup.java` | the committed-offset lookup (`lastCommittedEventId`) |
| `SseBackgroundEventHandler.java` | `onMessage`, `buildResponse` |

and, in another module:

| File | Part |
| --- | --- |
| `http-client/src/main/java/io/github/clescot/client/queue/QueueFactory.java` | `getQueue` (the queue implementation) |

**Sync commit: `739f94d20160b8f86d1871cb10fc6221e0fc40d3`** (2026-10-25, "fix: resume the SSE
source from the committed offset instead of \"now\""). At this commit the verification was
confirmed to describe the code above and prints `F1 LOOKS FIXED`, exit 3. The previous sync commit
was `826e024af0bb0015e9d3057a204216df90cd524d`, where the check was confirmed to describe the code
and printed the F1 baseline. Move this commit forward only after you have re-run the check and
confirmed the model still describes the code at the new commit, see "Keeping this in step with the
code".

## How to read the verdict

The script exits `3` when no event is lost under either server policy, `0` when the task
skips events under a "from now" policy, `1` on a new break, and `4` on a tool or model error.
For the code as it stands, `3` is the expected verdict.

| Verdict | Meaning | What to do |
| --- | --- | --- |
| `from_start : OK` and `from_now : OK` (exit 3) | No event is lost under either server policy: the task resumes from the committed offset. This is the expected state. | nothing, unless the covered code changed, then follow "Keeping this in step with the code" |
| `from_start : OK`, `from_now : VIOLATED` (exit 0) | The task resumes from "now" again: seeding the resume cursor is gone or no longer reaches the connection. This is what finding F1 was. | read `results/counterexample.txt`; the decoded trace names the step that drops the event, and `git log` over the covered files names the change that dropped the seeding |
| `from_start : VIOLATED` (exit 1) | **A new loss, independent of the resume seeding.** The guarded code was broken. | read `results/counterexample.txt`; the decoded trace names the step that drops the event |
| `ERROR` (exit 4) | TLC could not run, or `extract-facts.sh` could not find the code it reads. | fix the model or the script, not the test |

Telling a real bug from a modeling error:

- A violation whose trace contains **`the enqueue silently drops the event`** with
  `EnqueueLossless == FALSE` is a real bug: open `QueueFactory.getQueue` and
  `SseBackgroundEventHandler.onMessage` and check the queue type and `add`/`offer` call.
- A violation whose trace contains only steps the code cannot perform (for example `Drop`
  while the script printed `EnqueueLossless == TRUE`) is a **modeling error** — the model has
  drifted from the code. Fix `spec/SseDelivery.tla`, not the model checker.
- `extract-facts.sh: ... not found` / `cannot classify ...` means the code moved or changed
  shape. Update `scripts/extract-facts.sh` so it reads the new location; that is the intended
  maintenance path.

## The counterexample, in the code's words

`.agents/skills/verify-sse-source-no-event-loss/results/counterexample.txt` holds the violation
trace of the current run decoded into the connector's own steps, and holds no trace for a config
that passes. The trace below is the one the check produced for `from_now_restart` before the fix,
kept here as the record of what was repaired:

```
1. SseSourceTask.start(): task up, first SSE connection open
2. the SSE server publishes its next event
3. the worker loses the task: in-memory queue and uncommitted batch are gone
4. SseSourceTask.start(): a brand-new task instance comes up
5. SseConfiguration.connect(): opens a fresh BackgroundEventSource at the resume point
6. the SSE server publishes its next event
7. SseBackgroundEventHandler.onMessage(): the event is put on the in-memory queue
8. SseSourceTask.poll(): the whole queue is drained into SourceRecords
9. Kafka Connect commits the offsets of the returned records
```

That trace was finding F1b: the server published event 1 while the task was connected but before
event 1 was received, then the worker lost the task. The new task's fresh connection resumes at
"now" (`floor = 2`), skipping event 1; the server then publishes event 2, which is received,
polled and committed (`delivered = {2}`). Event 1 is now committed past and can never be
recovered. Longer traces in which `poll()` had already returned records that were never
committed are reachable too — the model explores them all.

Event ids in the trace are `1..MaxEvents` and correspond to the `id:` field of the SSE stream.

## Finding F1 (fixed in 739f94d2)

**F1 was: the connector never seeded the SSE resume cursor from the offset Kafka Connect
committed.** `SseSourceTask.poll` exported `{eventId: <id>}` as the source offset, but nothing
read committed offsets back, so `SseSourceTask.start` / `SseTask.connect` /
`SseConfiguration.connect` built a new `BackgroundEventSource` with no `offsetStorageReader` and
no `Last-Event-ID` request header. The only resume cursor was the eventsource library's in-memory
`lastEventId`, empty on a fresh task and until the first event of a connection arrives.

Consequence: an SSE server that resumes a connection with no `Last-Event-ID` from "now"
(a conforming choice — the SSE spec does not require a server to replay) will not re-send
events that arrived before the connection was re-established. The in-memory queue and the
polled-but-uncommitted batch are also lost on a restart or rebalance.

A loss is a loss: do not weaken `NoGap` or the model to make a violation pass. If exit 0 comes
back, the seeding was lost or broken, not the model. Two triggers were reproduced, and the fix
covers both:

- **F1a** — a connection drops before the first event of that connection arrives, then the
  library reconnects with no `Last-Event-ID`, so everything published during the gap is skipped.
- **F1b** — a task restart / rebalance: the worker loses the connection, the in-memory queue and
  any polled-but-uncommitted records, and the new task starts a fresh stream at "now".

The fix reads the committed offset per configuration with `context.offsetStorageReader()` in
`SseSourceTask`, hands it to `SseTask.connect` through `SseResumeLookup`, and seeds
`EventSource.Builder.lastEventId(...)` in `SseConfiguration.connect`, which is what puts
`Last-Event-ID` on the first request. The source partition moved from the event type to the
configuration id, so the offset can be read back at all.

**What the fix does not cover.** With no committed offset for a configuration, which is a
first-ever start and the first start after upgrading from type-keyed offsets, there is no portable
resume token. The connector sends no `Last-Event-ID` and inherits the server's policy, so events
published before the first commit are not recoverable. It logs a warning when that happens, and
`docs/sse_source_connector_settings.md` states the limit. `SeedsFromCommittedOffset` is a grep, so
the check prints `F1 LOOKS FIXED` whether or not this case is handled; only a live run against a
server that honours `Last-Event-ID` tells them apart.

## What the model is parameterised by (the code/model bridge)

`scripts/extract-facts.sh` reads two facts out of the Java source and writes them into a TLA+
`Facts` module; `SseDelivery.tla` cannot move without them. Each fact names the code it comes
from, so a code change that alters the behaviour either flips the fact or makes the script
fail loudly.

| Fact | Read from | `TRUE` means |
| --- | --- | --- |
| `EnqueueLossless` | `QueueFactory.getQueue` (queue type) + `SseBackgroundEventHandler.onMessage` (enqueue call) | the in-memory queue cannot silently drop an event |
| `SeedsFromCommittedOffset` | a call to `offsetStorageReader()` and, in the same three files, a seed call, either the builder's `lastEventId()` or a literal `"Last-Event-ID"` | the task reads what Kafka Connect committed and asks the server to resume from it. Both halves are required, so deleting either one flips the fact, and a comment cannot set it. It still cannot see whether the value seeded is the right one; see "Not covered by this check" |

`spec/Facts.tla` is a snapshot generated at the sync commit; `run-check.sh` always
regenerates it in a temporary directory, so the snapshot is for reading, not for checking.

## Bounds, assumptions, and what is not covered

Verified, within these bounds:

- At most `MaxEvents = 4` events are published (`spec/*.cfg`). Every interleaving of the
  actions in `spec/SseDelivery.tla` is explored, not sampled: with the resume cursor seeded all
  three configs search to completion with 0 states left on the queue (`from_start` and
  `from_now` 506 distinct states each, `from_now_restart` 156). `from_start` and `from_now` agree
  to the state, which is what the fix buys: the server's resume policy no longer changes what is
  delivered. Raise `MaxEvents` if a violation needs a longer trace, the state space grows quickly.
  Before the fix the same spec searched 1682 states for `from_start` and stopped at the violation
  after 137 (`from_now`) and 90 (`from_now_restart`) distinct states.
- The safety invariant `NoGap` in `SseDelivery.tla` is the at-least-once promise.

Assumptions the model makes about things it does not itself verify:

- The launchdarkly eventsource library advances `lastEventId` on every received event and
  sends `Last-Event-ID` on a reconnect that has one. `SseConfiguration.connect` does not
  override the request, so the model treats a mid-run reconnect as replay-from-`cursor`.
- The task seeds that cursor from the committed offset when a fresh task connects, and only then.
  The model's `SeedsFromCommittedOffset` branch is that step; the read from Connect's offset
  storage, the value it returns, and what the server does with the header are all outside the
  model.
- The server delivers events in increasing id order and the ids are contiguous.
- `SseSourceTask.poll` drains the entire in-memory queue into `SourceRecords` (the
  `while (queue.peek() != null) queue.poll()` loop) and Kafka Connect commits the returned
  offsets.
- Duplicates are allowed: the model checks *no loss*, not exactly-once. A replay after a
  reconnect can deliver the same event twice, and that is not a violation.

Not covered by this check:

- **Liveness / starvation.** `poll()` drains with `while (queue.peek() != null)`; whether a
  fast producer can keep `poll()` from ever returning is a separate property, not modelled here.
- **Duplicates / exactly-once.** Not checked.
- **Cross-task queue sharing.** `QueueFactory.getQueue` keys queues by configuration id in a
  `static` map, so two tasks in one JVM that use the same id share a queue. That is a separate
  target, not modelled here.
- **The cold start.** With no committed offset for a configuration the task sends no
  `Last-Event-ID`, so events published before its first commit are not recoverable. The model's
  `floor = 1` branch there describes a `from_start` server, not a portable guarantee, and the
  `SeedsFromCommittedOffset` grep cannot tell the two apart. One cold start per configuration also
  follows the partition key change from event type to configuration id, because the old offsets
  cannot be enumerated to migrate them.
- **Whether the server honours `Last-Event-ID`.** A server that ignores the header, or that treats
  an unknown id as "from now", still skips events published during the gap. The model assumes the
  header works.
- The read of the committed offset itself: the partition and offset keys, a failed or empty read,
  and the value a template would have produced for the id. The cursor uses the id the server sent.
- The template-processing path (`processEventWithTemplate`), the offset *payload* shape
  (`{configurationId, eventId}`), schema conversion, and anything outside the SSE source pipeline.
- The retry / backoff / error strategy and the circuit breaker.
- The HTTP sink connector and the in-memory queue source connector.

## Keeping this in step with the code

1. Change a covered file.
2. Run `bash .agents/skills/verify-sse-source-no-event-loss/scripts/run-check.sh`.
3. If it prints `from_start : VIOLATED`, the model caught a break — inspect it
   (`results/counterexample.txt`) before assuming the model is wrong.
4. If an actionable change was intended, update the model (add an action, or a `CONSTANTS`
   bound in the `.cfg`), then re-run until the verdict matches the intended behaviour.
5. Only then move the sync commit in this file forward to the new `git rev-parse HEAD`, and
   update the coverage table above if files or methods moved.

Suggested second target, if you want another check: the `SseTask.start` loop
(`SseTask.java`) calls `this.connect()` — which connects *every* configuration — inside the
per-configuration `forEach`, so a partially connected task can open a second
`BackgroundEventSource` for a configuration that is already connected, leaking the first
connection and duplicating its events. It is a small, concrete state machine and would suit a
second TLA+ model.
