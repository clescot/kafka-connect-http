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
verified), re-reads the facts from the Java source, runs TLC three times, writes the raw
results under `.agents/skills/verify-sse-source-no-event-loss/results/`, and prints a verdict.
Reads and writes only the paths above; it never changes product code.

**Read every result with the pstack `/statechart` skill.** It turns each invariant violation
into its shortest path in plain words and shows which violations a change fixes. Do not try to
read a TLC trace by hand: use `/statechart`.

## Does your change touch the verified code?

The check covers these files and methods — a change to any of them needs this check:

| File (all under `kafka-connect-http-connectors/src/main/java/io/github/clescot/kafka/connect/sse/client/okhttp/`) | Method / part |
| --- | --- |
| `SseSourceTask.java` | `start`, `poll`, `stop` |
| `SseTask.java` | `connect`, `start` |
| `SseConfiguration.java` | `connect` (the `BackgroundEventSource` builder, retry/error strategy, `ConnectStrategy`) |
| `SseBackgroundEventHandler.java` | `onMessage`, `buildResponse` |

and, in another module:

| File | Part |
| --- | --- |
| `http-client/src/main/java/io/github/clescot/client/queue/QueueFactory.java` | `getQueue` (the queue implementation) |

**Sync commit: `826e024af0bb0015e9d3057a204216df90cd524d`** (2026-10-24, "Add Exchange
templating system (HTTP + SSE) and split core/client modules (#947)"). At this commit the
verification was confirmed to describe the code above. Move this commit forward only after you
have re-run the check and confirmed the model still describes the code at the new commit — see
"Keeping this in step with the code".

## How to read the verdict

The script exits `0` on the known baseline, `1` on a new break, `3` when finding F1 appears
fixed, and `4` on a tool/model error.

| Verdict | Meaning | What to do |
| --- | --- | --- |
| `from_start : OK` | With a server that replays from the start of the stream, no event is lost. | nothing |
| `from_start : VIOLATED` (exit 1) | **A new loss, independent of finding F1.** The guarded code was broken. | read `results/counterexample.txt` with `/statechart`; the trace names the step that drops the event |
| `from_now : VIOLATED` | Finding F1 is still present (see below). | this is the baseline |
| `F1 LOOKS FIXED` (exit 3) | No event is lost under either server policy. | remove finding F1 from this skill and move the sync commit forward |
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
trace decoded into the connector's own steps, for example:

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

That trace is finding F1b: the server published event 1 while the task was connected but before
event 1 was received, then the worker lost the task. The new task's fresh connection resumes at
"now" (`floor = 2`), skipping event 1; the server then publishes event 2, which is received,
polled and committed (`delivered = {2}`). Event 1 is now committed past and can never be
recovered. Longer traces in which `poll()` had already returned records that were never
committed are reachable too — the model explores them all.

Event ids in the trace are `1..MaxEvents` and correspond to the `id:` field of the SSE stream.

## Finding F1 (reported, not fixed)

**F1: the connector never seeds the SSE resume cursor from the offset Kafka Connect
committed.** `SseSourceTask.poll` exports `{eventId: <id>}` as the source offset, but nothing
reads committed offsets back: `SseSourceTask.start` / `SseTask.connect` / `SseConfiguration.connect`
build a new `BackgroundEventSource` without `offsetStorageReader` and without a `Last-Event-ID`
request header. The only resume cursor is the eventsource library's in-memory `lastEventId`,
which is empty on a fresh task and until the first event of a connection arrives.

Consequence: an SSE server that resumes a connection with no `Last-Event-ID` from "now"
(a conforming choice — the SSE spec does not require a server to replay) will not re-send
events that arrived before the connection was re-established. The in-memory queue and the
polled-but-uncommitted batch are also lost on a restart or rebalance.

This is a finding, not a reason to relax the check: do not weaken `NoGap` or the model to make
`from_now` pass. Two triggers are reproduced:

- **F1a** — a connection drops before the first event of that connection arrives, then the
  library reconnects with no `Last-Event-ID`, so everything published during the gap is skipped.
- **F1b** — a task restart / rebalance: the worker loses the connection, the in-memory queue and
  any polled-but-uncommitted records, and the new task starts a fresh stream at "now".

A fix would be: in the source task's `start`, read the committed offset with
`context.offsetStorageReader()` and seed the resume cursor (a `Last-Event-ID` request header
via `SseConfiguration.connect`'s `requestTransformer`, or reject a resume policy that cannot
replay). After such a change, the script should print `F1 LOOKS FIXED` and exit 3.

## What the model is parameterised by (the code/model bridge)

`scripts/extract-facts.sh` reads two facts out of the Java source and writes them into a TLA+
`Facts` module; `SseDelivery.tla` cannot move without them. Each fact names the code it comes
from, so a code change that alters the behaviour either flips the fact or makes the script
fail loudly.

| Fact | Read from | `TRUE` means |
| --- | --- | --- |
| `EnqueueLossless` | `QueueFactory.getQueue` (queue type) + `SseBackgroundEventHandler.onMessage` (enqueue call) | the in-memory queue cannot silently drop an event |
| `SeedsFromCommittedOffset` | `offsetStorageReader` / `Last-Event-ID` anywhere in `SseSourceTask.java`, `SseTask.java`, `SseConfiguration.java` | the task asks the server to resume from the committed offset |

`spec/Facts.tla` is a snapshot generated at the sync commit; `run-check.sh` always
regenerates it in a temporary directory, so the snapshot is for reading, not for checking.

## Bounds, assumptions, and what is not covered

Verified, within these bounds:

- At most `MaxEvents = 4` events are published (`spec/*.cfg`). Every interleaving of the
  actions in `spec/SseDelivery.tla` is explored, not sampled: the `from_start` config searches
  1682 distinct states to completion (0 left on the queue); the two `from_now` configs stop as
  soon as they violate (137 and 90 states before the violation). Raise `MaxEvents` if a
  violation needs a longer trace — the state space grows quickly.
- The safety invariant `NoGap` in `SseDelivery.tla` is the at-least-once promise.

Assumptions the model makes about things it does not itself verify:

- The launchdarkly eventsource library advances `lastEventId` on every received event and
  sends `Last-Event-ID` on a reconnect that has one. `SseConfiguration.connect` does not
  override the request, so the model treats a mid-run reconnect as replay-from-`cursor`.
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
- The template-processing path (`processEventWithTemplate`), the offset *payload* shape
  (`{type, eventId}`), schema conversion, and anything outside the SSE source pipeline.
- The retry / backoff / error strategy and the circuit breaker.
- The HTTP sink connector and the in-memory queue source connector.

## Keeping this in step with the code

1. Change a covered file.
2. Run `bash .agents/skills/verify-sse-source-no-event-loss/scripts/run-check.sh`.
3. If it prints `from_start : VIOLATED`, the model caught a break — inspect it with
   `/statechart` before assuming the model is wrong.
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
