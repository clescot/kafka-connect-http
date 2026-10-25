------------------------------ MODULE SseDelivery ------------------------------
(***************************************************************************)
(* A model of the event-delivery pipeline of the SSE source connector.      *)
(*                                                                          *)
(* It answers one question: can an SSE event that the server publishes      *)
(* ever be skipped, so that a *later* event reaches Kafka while the         *)
(* skipped one never does?                                                  *)
(*                                                                          *)
(* Every action below names the real code it mirrors. Read the SKILL.md     *)
(* next to this file for the coverage record and the assumptions.           *)
(*                                                                          *)
(* Mirrored code (all under kafka-connect-http-connectors/src/main/java/     *)
(* io/github/clescot/kafka/connect/sse/client/okhttp/):                     *)
(*   SseSourceTask.start            -> task starts, connects, resumes       *)
(*   SseSourceTask.poll             -> Poll / Commit                          *)
(*   SseTask.connect / start        -> Connect                                *)
(*   SseConfiguration.connect       -> builds a fresh BackgroundEventSource  *)
(*   SseBackgroundEventHandler.onMessage -> Receive / Drop                    *)
(*   QueueFactory.getQueue          -> the in-memory `queue`                  *)
(*                                                                          *)
(* Facts.tla (generated from that Java source by scripts/extract-facts.sh)  *)
(* supplies the two facts this model is parameterised by.                   *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC, Facts

CONSTANTS
    MaxEvents,        \* how many event ids the server may publish (1..MaxEvents)
    ServerResume,     \* "from_start" | "from_now": what the server does when a
                      \* fresh connection carries no Last-Event-ID
    AllowMidRunReconnect  \* TRUE: the connection may drop while the task stays
                      \* up (the library reconnects). FALSE isolates the
                      \* restart/rebalance trigger, where the in-memory queue
                      \* and the uncommitted batch are lost.

VARIABLES
    published,        \* set of event ids the SSE server has produced
    cursor,           \* highest id the live connection has accounted for
                      \* (received, or skipped because the connection resumed past it)
    floor,            \* lowest id the current connection may still deliver
    queue,            \* ids received and waiting in the in-memory queue
    batch,            \* ids already drained into SourceRecords, not yet committed
    delivered,        \* ids already committed to Kafka
    running,          \* the source task is up
    connected         \* the SSE connection is open

vars == <<published, cursor, floor, queue, batch, delivered, running, connected>>

Events == 1..MaxEvents

MaxOr0(S) == IF S = {} THEN 0 ELSE CHOOSE m \in S : \A x \in S : x <= m

(***************************************************************************)
(* The connector has started and opened its first connection. Nothing has    *)
(* been published yet, so every resume policy starts delivery at id 1.       *)
(***************************************************************************)
Init ==
    /\ published = {}
    /\ cursor = 0
    /\ floor = 1
    /\ queue = {}
    /\ batch = {}
    /\ delivered = {}
    /\ running = TRUE
    /\ connected = TRUE

(***************************************************************************)
(* The server publishes its next event. It does this whether or not the      *)
(* connector is up.                                                          *)
(***************************************************************************)
Publish(e) ==
    /\ e \in Events
    /\ e = Cardinality(published) + 1
    /\ published' = published \cup {e}
    /\ UNCHANGED <<cursor, floor, queue, batch, delivered, running, connected>>

(***************************************************************************)
(* SseConfiguration.connect(): opens a fresh BackgroundEventSource.          *)
(* The resume point depends on what the client asks for:                     *)
(*   - mid-run reconnect: the library replays from its in-memory             *)
(*     Last-Event-ID, i.e. cursor > 0.                                       *)
(*   - after a restart: cursor is 0 (in-memory state is gone), so the        *)
(*     client either seeds Last-Event-ID from the committed offset           *)
(*     (SeedsFromCommittedOffset) or sends no Last-Event-ID at all and the   *)
(*     server falls back to its own policy (ServerResume).                   *)
(***************************************************************************)
Connect ==
    /\ running /\ ~connected
    /\ LET newFloor == IF cursor > 0
                      THEN cursor + 1
                      ELSE IF SeedsFromCommittedOffset
                           THEN MaxOr0(delivered) + 1
                           ELSE IF ServerResume = "from_start"
                                THEN 1
                                ELSE Cardinality(published) + 1
       IN  /\ floor' = newFloor
           /\ cursor' = newFloor - 1
    /\ connected' = TRUE
    /\ UNCHANGED <<published, queue, batch, delivered, running>>

(***************************************************************************)
(* SseBackgroundEventHandler.onMessage(): the connection hands the next      *)
(* event to the handler, which puts it on the in-memory queue.               *)
(***************************************************************************)
Receive(e) ==
    /\ running /\ connected
    /\ e \in published
    /\ e >= floor
    /\ e = cursor + 1
    /\ queue' = queue \cup {e}
    /\ cursor' = e
    /\ UNCHANGED <<published, floor, batch, delivered, running, connected>>

(***************************************************************************)
(* Only when EnqueueLossless is FALSE: the enqueue silently drops the event  *)
(* (a bounded queue used with offer()). The connection still advances, so    *)
(* the event is gone for good.                                               *)
(***************************************************************************)
Drop(e) ==
    /\ ~EnqueueLossless
    /\ running /\ connected
    /\ e \in published
    /\ e >= floor
    /\ e = cursor + 1
    /\ cursor' = e
    /\ UNCHANGED <<published, floor, queue, batch, delivered, running, connected>>

(***************************************************************************)
(* SseSourceTask.poll(): drains the whole queue into SourceRecords. An       *)
(* event is not durable yet.                                                 *)
(***************************************************************************)
Poll ==
    /\ running
    /\ batch' = batch \cup queue
    /\ queue' = {}
    /\ UNCHANGED <<published, cursor, floor, delivered, running, connected>>

(***************************************************************************)
(* Kafka Connect commits the offsets of the returned records.                *)
(***************************************************************************)
Commit ==
    /\ running
    /\ delivered' = delivered \cup batch
    /\ batch' = {}
    /\ UNCHANGED <<published, cursor, floor, queue, running, connected>>

(***************************************************************************)
(* The connection drops while the task keeps running (library reconnects).   *)
(***************************************************************************)
Disconnect ==
    /\ AllowMidRunReconnect
    /\ running /\ connected
    /\ connected' = FALSE
    /\ UNCHANGED <<published, cursor, floor, queue, batch, delivered, running>>

(***************************************************************************)
(* The worker loses the task: the connection, the queue and the polled-but-  *)
(* uncommitted batch are all in memory, so they are gone. What was committed *)
(* to Kafka survives.                                                        *)
(***************************************************************************)
Crash ==
    /\ running
    /\ running' = FALSE
    /\ connected' = FALSE
    /\ cursor' = 0
    /\ floor' = 0
    /\ queue' = {}
    /\ batch' = {}
    /\ UNCHANGED <<published, delivered>>

(***************************************************************************)
(* SseSourceTask.start(): a new task instance comes up. It builds a new      *)
(* SseTask and connects; nothing is remembered except what Kafka stored.     *)
(***************************************************************************)
Restart ==
    /\ ~running
    /\ running' = TRUE
    /\ connected' = FALSE
    /\ UNCHANGED <<published, cursor, floor, queue, batch, delivered>>

Stutter == UNCHANGED vars

Next ==
    \/ \E e \in Events : Publish(e)
    \/ Connect
    \/ \E e \in Events : Receive(e)
    \/ \E e \in Events : Drop(e)
    \/ Poll
    \/ Commit
    \/ Disconnect
    \/ Crash
    \/ Restart
    \/ Stutter

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* NoGap is the at-least-once promise, stated as a safety invariant:         *)
(* every event the server published up to the highest committed id has been  *)
(* committed. A violation means the connector skipped an event and then      *)
(* delivered a later one.                                                    *)
(***************************************************************************)
NoGap ==
    /\ delivered \subseteq published
    /\ \A e \in published : e <= MaxOr0(delivered) => e \in delivered

=============================================================================
