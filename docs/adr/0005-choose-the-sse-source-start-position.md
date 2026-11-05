# Choose the SSE source start position

The SSE source resumes a stream from the event id Kafka Connect committed for its configuration, so a
restart or a rebalance re-delivers what was published in the gap. A configuration with no committed
offset has no resume token to send: the connector sends no `Last-Event-ID`, the server's own policy
applies, and for most servers that means "from now". The events published before the first commit are
not recoverable. This is a cold start, and there are two of them — a first-ever start, and one window
per configuration on the upgrade that moved the source partition from the event type to the
configuration id, whose old offsets cannot be enumerated and therefore cannot be migrated.

Whether a server replays when asked is unobservable from the connector: at a cold start it has no
prior id, and no response tells it whether a header it never sent would have been honoured. The
operator is the only one who knows.

Each configuration therefore carries a Resume Policy, `config.<id>.resume.policy`, that says what the
connector asks its server for in a cold start. `from-committed-offset`, the default, asks nothing and
reproduces the old behaviour. `from-start` asks for the whole stream by sending
`config.<id>.resume.from.start.event.id` (default `0`) as `Last-Event-ID`, which is a per-server
convention rather than a standardised request. `fail` refuses to start the task, naming the
configuration, so the gap becomes an operator's problem in the log instead of silent data loss. An
offset, once committed, wins under every policy: the policy fires at most once per configuration, at
its cold start.

`from-start` is applied inside the existing `requestTransformer`, adding the sentinel only when the
request carries no `Last-Event-ID` of its own. An absent header is how the eventsource library says
"nothing to resume from", so the library's in-memory cursor always wins and a mid-run reconnect
resumes from it as before. An empty sentinel is a real value: it sends the header with no value, the
convention some servers use for "the whole stream".

## Consequences

- A `from-start` configuration may receive duplicates at its cold start, which the connector's
  no-loss promise allows, and only then.
- A `fail` configuration stops every stream of its task, not just the one without an offset: a source
  task owns several configurations and Connect has no per-configuration failure channel. A task that
  Connect never initialised, and so has no offset storage at all, is not a cold start: `fail` does
  not fire there.
- A static `Last-Event-ID` header is re-sent on every reconnect, so it defeats the in-memory cursor
  and replays from that position forever. Combined with `from-start` it is refused at configuration
  time; on its own it is accepted with a warning.
- The TLA+ check of the no-loss promise is unchanged. Its `from_start` configuration already
  describes the opted-in world and its `from_now` configuration the world where nobody opted in, and
  the policy is a runtime choice its fact extractor cannot see.
- The orphaned type-keyed offsets stay orphaned. Nothing migrates them, and the upgrade costs one
  cold start per configuration.

## Considered options

- **Replay from the beginning on every task start.** A useful rescue hatch, but it turns every
  restart into a duplicate storm and gives one setting two meanings. Rejected; a separate switch if
  the need ever appears.
- **Failing the task as the default.** It would break every first-ever start, where a stream simply
  has no beginning to resume from. Rejected; the policy is opt-in.
- **A separate boolean for the failure mode.** It cannot express more than `fail` does — the
  connector cannot detect that a server ignored the sentinel — so it would only multiply the
  meaningless combinations. Rejected in favour of one three-valued setting.
- **Detecting the server's replay behaviour.** Impossible at a cold start for the reasons above.
  Rejected; the connector asks, the operator declares.
- **Expressing `from-start` with the existing static header enrichment.** It is possible today, and
  it is exactly what the new warning warns about: the static header is re-applied on every reconnect
  and overrides the cursor. Rejected.
- **Migrating the old offsets.** The pre-change partition was the event type, which is only
  discovered from the stream, so the old partitions cannot be enumerated. Rejected.
