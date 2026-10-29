# Expose the record event time to sink JEXL expressions

Sink JEXL expressions could only reach a date and time through `sinkRecord.timestamp()`, a raw
epoch-millis value that had to be formatted inline. The sink JEXL context now also carries
`timestamp` (epoch seconds), `timestampInMs` (epoch millis) and `dateTime` (ISO-8601 in UTC),
built from the record's event time, plus a `datetime` namespace for
`DateTimeFormatter` patterns. The values fall back to the evaluation time when a record carries no
event time, and everything is formatted in UTC.

## Consequences

- The three variables are available to every sink-side JEXL context (request mappers and message
  splitters), because they share one context builder.
- The JEXL engine registers the `datetime` namespace and permits `DateTimeFunctions`, so the
  engine setup now lives in `JexlEngineFactory` rather than in `HttpSinkTask`; tests build the same
  engine as production.
- The event time is reproducible across replays as long as the record carries one; a record without
  an event time uses the evaluation time, so its matcher and mapper can differ by a few
  milliseconds.
- `dateTime` uses UTC and the English locale, while the `${datetime:...}` exchange templates use the
  system default zone and the default locale — and render the offset as `+0000` where `dateTime`
  renders `Z`. The two vocabularies therefore disagree on both zone and locale; aligning them is a
  separate change.
- `datetime:format` accepts only ISO-8601 strings: a bare epoch value has no unit, so
  `datetime:fromEpochMillis` exists instead of guessing between seconds and milliseconds.

## Considered options

- **Wall-clock "now" as the source.** Rejected: replaying a topic would send different URLs, and
  `sinkRecord.timestamp()` already carries the intended instant.
- **The system default zone and locale, like the `${datetime:...}` templates.** Rejected: the
  reference example is `2023-11-06T00:00:00.000Z`, and host-dependent output is a poor default for a
  URL or header.
- **Reusing `${datetime:...}` inside JEXL string literals.** Rejected: the sink request path never
  runs through `ExchangeTemplateManager` — templates are resolved on the response body only — so
  this would have required wiring template resolution into request mapping.
- **A `datetime` variable object instead of a namespace.** Rejected: it would sit next to `dateTime`
  with a confusing one-letter case difference, and the namespace matches the existing `${datetime:...}`
  vocabulary.
