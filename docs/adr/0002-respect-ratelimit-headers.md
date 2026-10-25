# Respect the IETF RateLimit response headers

Some servers advertise their quota in the IETF `RateLimit` / `RateLimit-Policy` response fields
(draft-ietf-httpapi-ratelimit-headers). The HTTP sink connector now reads them in
`HttpConfiguration`: when a `requests` or `concurrent-requests` service limit is exhausted
(`r=0`), the next calls to the same configuration + target authority pause for the advertised
window. Waits under `config.<id>.rate.limit.headers.max.threshold.in.sec` happen inline through
a per-configuration gate (shared by the task's per-user clients, and JVM-wide per configuration
id with `scope: static`); longer windows open the circuit breaker, exactly like an over-threshold
`Retry-After`. `Retry-After` / `X-Retry-After` always wins when both fields are present, as the
draft requires.

## Consequences

- The behaviour is opt-out: it only triggers when a server explicitly advertises an exhausted
  quota, and `config.<id>.rate.limit.headers.enabled: false` restores the previous behaviour.
- The pause is keyed by configuration + authority and jittered by up to 500 ms on resume, so
  several tasks do not resume against the same server at the same instant.
- `RateLimit-Policy` is used only to resolve the quota unit and the `w` fallback; policy details
  are otherwise informational, as the draft allows. Malformed fields are ignored.
- The same per-configuration plumbing work makes the documented
  `config.<id>.retry.policy.*` settings and `config.<id>.exchange.template` effective for the
  sink, which the reference had promised but the runtime had ignored.

## Considered options

- **Preemptive pacing from the advertised `r`/`t` rate.** Rejected for now: it needs
  partition-key tracking and buys little over wait-on-exhaustion. It remains a follow-up.
- **Treating the fields exactly like `Retry-After`, on the retry status codes only.** Rejected:
  the draft allows the fields on any response, and gating future calls (rather than retrying the
  current exchange) is what "do not exceed the available quota" means.
- **Implementing the de-facto `X-RateLimit-*` headers as well.** Rejected for now: their
  semantics vary wildly; the parser seam allows adding an adapter later.
