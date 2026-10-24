# Customize exchange templates

Before an exchange is published, the connector can rewrite its content with a template. This
guide shows how to enable templating, restrict which processors are allowed, and solve common
reshaping tasks.

For the full grammar of every processor, see
[exchange template syntax](../reference/exchange-template-syntax.md).

## How templating works

A template is a string that mixes literal text with `${processor:expression}` placeholders.
The connector evaluates each placeholder against the exchange, concatenates the results with
the surrounding text, and sets the resulting string as the **content** of the published
exchange (the response body for HTTP, the event data for SSE).

```
status=${jsonpath:$.response.statusCode} url=${jsonpath:$.request.url}
```

## Enable templating

Templating is disabled by default. Set `exchange.template` to turn it on:

```json
{ "exchange.template": "status=${jsonpath:$.response.statusCode} ${random.uuid}" }
```

- **HTTP Sink** — placeholders are evaluated against the `HttpExchange`.
- **SSE Source** — placeholders are evaluated against each received event.
- **HTTP Source** — templating has **no effect**: it only consumes exchanges that were
  already templated on the producing side (the HTTP Sink connector).

To scope a template to one configuration, prefix it:
`config.<id>.exchange.template`.

## Restrict the enabled processors

By default every built-in processor is enabled. Set `exchange.template.processors` to a
comma-separated allow-list to keep templates predictable and to make typos fail fast:

```json
{
  "exchange.template": "${jsonpath:$.response.statusCode}",
  "exchange.template.processors": "jsonpath"
}
```

If a name is misspelled or unknown, the connector rejects the configuration at start-up with
a message listing the known processors. An empty value means "all built-in processors".

Built-in processors: `jsonpath`, `xpath`, `jmespath`, `regex`, `header`, `param`, `cookie`,
`datetime`, `if`, `hash`, `math`, `random`. (`headerparam` and `conditional` are accepted as
aliases at validation time; the placeholders themselves are `${header:...}`, `${param:...}`,
`${cookie:...}`, and `${if:...}`.) Custom processors can be added through the Java Service
Loader and then listed here by name.

## Where the data lives

The placeholders navigate the exchange, not the raw HTTP body. For an HTTP exchange the
navigable root is:

```json
{
  "request":  { "url": "...", "method": "...", "headers": {...}, "body": "...", "attributes": {...} },
  "response": { "statusCode": 200, "statusMessage": "OK", "headers": {...}, "body": "...", "contentType": "..." },
  "durationInMillis": 42,
  "moment": "...",
  "attempts": 1,
  "success": true,
  "content": "...",
  "attributes": {...}
}
```

So use `$.request.*` and `$.response.*` — **not** `$.httpRequest.*` or `$.httpResponse.*`.
For an SSE exchange the root exposes `eventId`, `eventType`, `success`, `requestUrl`,
`requestMethod`, `content`, and `attributes`.

## Common tasks

### Extract a field from the exchange

```json
{ "exchange.template": "${jsonpath:$.response.statusCode}" }
```

### Combine request and response data

```json
{
  "exchange.template": "${jsonpath:$.request.url} -> ${jsonpath:$.response.statusCode}"
}
```

### Add a timestamp

```json
{
  "exchange.template": "${jsonpath:$.response.body} at ${datetime:now:yyyy-MM-dd HH:mm:ss}"
}
```

### Generate a unique id

```json
{ "exchange.template": "${random.uuid}" }
```

### Apply conditional logic

```json
{
  "exchange.template": "${if:status:200:ok:ko}"
}
```

This yields `ok` when the response status is `200`, and `ko` otherwise. See the
[`if` reference](../reference/exchange-template-syntax.md#if) for status ranges, attribute
conditions, and comparisons.

### Hash a value

```json
{ "exchange.template": "${hash:SHA-256:some-literal-to-hash}" }
```

The hash processor hashes the literal text you give it. Supported algorithms: `MD5`,
`SHA-1`, `SHA-256`, `SHA-384`, `SHA-512`, `SHA3-256`, `SHA3-384`, `SHA3-512`.

### Produce structured JSON

Templates concatenate strings, so build JSON directly in the template:

```json
{
  "exchange.template": "{\"url\":\"${jsonpath:$.request.url}\",\"status\":${jsonpath:$.response.statusCode}}"
}
```

## Verify your template

- Keep the processor list as small as the task allows; unknown processors then fail at
  start-up instead of silently leaving an unresolved placeholder.
- Test on a single request first and inspect the message in the success topic.
- Extract only fields that exist in the exchange root (see above).
- Enable debug logging for the template engine:
  `log4j.logger.io.github.clescot.core.http.template=DEBUG`.

## Next steps

- Full processor syntax: [exchange template syntax](../reference/exchange-template-syntax.md).
- The published exchange is a `Struct`; its fields are listed in
  [message formats](../reference/message-formats.md).
