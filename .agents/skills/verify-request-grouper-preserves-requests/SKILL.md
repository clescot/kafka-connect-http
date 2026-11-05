---
name: verify-request-grouper-preserves-requests
description: "Exhaustively check that the HTTP sink's RequestGrouper keeps every pending request exactly once - no loss, duplication, invented group or misrouted group - and always terminates. Use when a change touches RequestGrouper, RequestGrouperFactory, HttpTask.groupRequests, or the `request.grouper.*` settings."
---

# Verify: request grouping preserves every pending request

**Property (one sentence).** For every list of pending requests and every `message.limit` /
`body.limit` setting, `RequestGrouper.group` returns requests in which each input request appears
exactly once — inside one aggregated body or on its own — every aggregate carries the URL of a
matching request, no empty aggregate is invented, and the call terminates.

**Harm (one sentence).** A duplicated or invented request sends the same record to the server
twice; a lost one never reaches it; an aggregate sent to a non-matching URL posts one endpoint's
payload to another; a `StackOverflowError` kills the sink task.

The check calls the **real** `RequestGrouper.group` — there is no model of the algorithm, so it
cannot drift from the code. It enumerates a finite input space instead of sampling: 5440 cases, in
about 2 s.

## Run it

```bash
bash .agents/skills/verify-request-grouper-preserves-requests/scripts/run-check.sh
```

One command. It builds `http-client` and `http-core` with Maven (installing the `http-core`
snapshot in the local repository so the classpath resolves), compiles
`checker/GrouperChecker.java` against the real class, runs every case in one JVM, writes the result
to `results/check.txt`, and prints a verdict. It reads only the module sources and writes only
`results/` and the local Maven repository; it never changes product code.

## Does your change touch the checked code?

| File (under `http-client/src/main/java/io/github/clescot/client/http/`) | Part |
| --- | --- |
| `RequestGrouper.java` | `group(List)`: the matching / non-matching split, the `message.limit` / `body.limit` loop, how many matching entries each aggregate consumes, the aggregate seed and the `start` / `separator` / `end` assembly |
| `RequestGrouperFactory.java` | the values the factory feeds it: the predicate, `message.limit` (`-1` when absent), `body.limit` (`-1` when absent), the `separator` / `start` / `end` defaults |
| `HttpTask.java` | `groupRequests`, the caller that applies each registered grouper in turn |

**Sync commit: `d1d28e4e`** (2026-10-29, the #993 grouper fix). At this commit the check reports
`no violation` on all 5440 cases. The previous anchor, `1bfd7493` (master after the 0.8.192
release), reported the four findings below in 4472 cases; the fix is the one described under
"Findings fixed at the sync commit". Move this commit forward only after you have re-run the check
and confirmed what it reports at the new commit.

## How to read the verdict

| Exit | `results/check.txt` | Meaning | What to do |
| --- | --- | --- | --- |
| `0` | `no violation` | Every request came out exactly once, on a matching URL, and every case terminated. | nothing |
| `1` | `N violating case(s) {cause=count, …}` then the first 12 cases | The grouper breaks the promise. These are findings about the code. | read the cases; compare with "Findings" below; do not weaken the checker or its input space |
| `4` | `run-check: …` | The check could not run: the module did not build, the classpath did not resolve, or `RequestGrouper`'s API moved. | fix the build; if the API moved on purpose, update `checker/GrouperChecker.java` to the new API |

The checker names each violation by cause:

- `duplicates a request` — a body appears more times in the output than in the input.
- `loses a request` — a body appears fewer times, or not at all.
- `invents an empty request` — the grouper produced an aggregate with an empty body.
- `wrong destination` — an aggregate's URL is not the URL of any matching request.
- `does not terminate` — the recursion never shrank the list (`StackOverflowError`).

## Findings fixed at the sync commit

Before `d1d28e4e`, all four were one algorithm: `group()` filtered `matchingEntries` /
`nonMatchingEntries`, walked `matchingEntries`, set `consumed = i` when a limit was hit, then
recursed on `entries.subList(consumed, entries.size())` — an index into `matchingEntries` used on
`entries` — and added `nonMatchingEntries` again at every level. The fix builds the groups
iteratively from `matchingEntries`: each aggregate is cloned from its first matching entry,
`message.limit` counts within the group, a group always consumes at least its first entry, and the
non-matching entries are added once at the end.

- **F1 — duplicated requests.** `entries=[N(x), M(a), M(bb)]`, `message.limit=1`: bodies came back
  as `a, a, bb, x`. The index space mismatch re-processed the same matching entry.
- **F2 — misrouted aggregates.** `entries=[N(y), M(a)]`, no limits: the group `[a]` was sent to
  `http://other.example/0`, the non-matching entry's URL, because the aggregate was cloned from
  `entries.get(0)` instead of the first *matching* entry.
- **F3 — invented empty request.** `entries=[N(x), N(y)]`: the grouper returned both originals
  **and** an empty group `[]`, sent to the first non-matching URL. It also did this whenever no
  entry matched.
- **F4 — non-termination.** `entries=[M(bb)]`, `body.limit=3`: the first matching body already
  reached the limit, so `consumed` stayed `0` and `group()` recursed on the same list until
  `StackOverflowError` — in a live sink, that killed the task.

Counts at `1bfd7493`: `does not terminate=2120`, `wrong destination=1620`,
`invents an empty request=480`, `duplicates a request=252`; **no case loses a body** in the
enumerated space. At `d1d28e4e` the same check reports no violation. Evidence:
`results/check.txt`.

The algorithm first landed in `7415223c6` ("336 batch messages into one http request body (#534)");
`git log --follow -- http-client/src/main/java/io/github/clescot/client/http/RequestGrouper.java`
tracks it since.

## The code → check bridge

- The checker is a Java program compiled against the real class: `new RequestGrouper<>(id, predicate,
  separator, start, end, messageLimit, bodyLimit)` and `group(List<Pair<T, HttpRequest>>)`. A rename,
  a moved package or a changed constructor breaks compilation, and `run-check.sh` reports it as exit
  4 ("Update checker/GrouperChecker.java"), so the check cannot silently keep passing against code it
  no longer reads.
- No fact is extracted by pattern matching: the checker calls the class. The input space is the only
  abstraction, and it is spelled out as constants at the top of `checker/GrouperChecker.java`
  (`KINDS`, `MESSAGE_LIMITS`, `BODY_LIMITS`, `MAX_LENGTH`).
- The suspended entries are `Pair<String, HttpRequest>` with a URL that the predicate accepts
  (`http://match.example/…`) or rejects (`http://other.example/…`), body tokens `a`, `bb`, `x`, `y`,
  `separator="|"`, `start="["`, `end="]"`. The checker parses grouped bodies back into tokens, so
  conservation is checked on the payload, not on the pair keys (the aggregate deliberately keeps the
  first record's key).

## Bounds, assumptions, and what is not covered

Verified, within these bounds:

- At most 4 entries per call, drawn from 4 kinds (matching short `a`, matching longer `bb`,
  non-matching `x` and `y`); body tokens are 1–2 characters.
- `message.limit` ∈ {-1, 1, 2, 3} and `body.limit` ∈ {-1, 2, 3, 5}, with `-1` meaning "no limit" as
  in `RequestGrouperFactory`. Every combination is run for every sequence: 5440 cases, exhaustive.
- One grouper per call, as `HttpTask.groupRequests` invokes them. Bodies are non-null strings.
- `body.limit` is compared against `String.length()` (the code), not UTF-8 bytes (what
  `docs/reference/http-sink-connector.md` says), so the bound is in characters. The check runs
  before adding the next body and ignores the separator and `end`, so an assembled body can exceed
  the configured limit; that is existing behaviour, not something this property checks.

Assumptions the check makes:

- `RequestGrouper` is the grouping decision: the predicate, limits and body assembly are its own.
  The predicate itself (regex matching, header matching in `HttpRequestPredicateBuilder`) is not
  verified — the checker uses a URL-prefix predicate.
- The aggregate's request fields other than the body are those of the request it is cloned from,
  which is what `RequestGrouper` does; the checker only asserts *which* URL that is.

Not covered by this check:

- **More than 4 entries, or other limit values.** A violation may need a longer list; raise
  `MAX_LENGTH` or the limit arrays in `checker/GrouperChecker.java` and re-run (the space grows as
  4ⁿ, so keep it small).
- **Composing several groupers.** `HttpTask.groupRequests` reduces over the registered groupers;
  the second grouper sees the first one's output. The checker runs one grouper.
- **Null bodies.** The checker only uses non-null strings. Since `d1d28e4e`, `group()` treats a
  null `bodyAsString` as empty (no `NullPointerException` with `body.limit`, no literal `null`);
  `RequestGrouperTest` covers both shapes. Null bodies are still outside the enumerated property.
- **Non-`STRING` body types, byte arrays, multipart and form bodies**; the grouper concatenates
  `getBodyAsString()` only.
- The HTTP call itself, retries, rate limiting, and the sink's mapper/splitter stages.
