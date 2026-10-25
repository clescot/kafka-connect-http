# Exchange template syntax

Complete reference for the `${processor:expression}` placeholders accepted by the
`exchange.template` setting. For a task-oriented introduction, see
[customize exchange templates](../how-to/customize-exchange-templates.md).

## Template format

A template is a plain string that may contain any number of placeholders:

```
text ${jsonpath:$.request.url} more text ${random.uuid}
```

The connector parses the string, evaluates each placeholder against the exchange, and
concatenates the results with the literal text. The final string becomes the exchange
**content**. Placeholders may be nested; the evaluator resolves inner ones first and stops at
a depth of 100.

Which processors are available is controlled by
[`exchange.template.processors`](http-sink-connector.md#exchange-templating). An empty value
enables all built-in processors.

## The exchange root

Placeholders navigate the exchange, not the raw HTTP body.

**HTTP exchange**

| Path | Description |
| --- | --- |
| `$.request.url` | Request URL. |
| `$.request.method` | HTTP method. |
| `$.request.headers` | Request headers (map of arrays). |
| `$.request.body` | Request body. |
| `$.request.attributes` | Request attributes. |
| `$.response.statusCode` | HTTP status code. |
| `$.response.statusMessage` | HTTP status message. |
| `$.response.headers` | Response headers (map of arrays). |
| `$.response.body` | Response body. |
| `$.response.contentType` | Response content type. |
| `$.durationInMillis` | Exchange duration. |
| `$.moment` | Exchange timestamp. |
| `$.attempts` | Number of attempts. |
| `$.success` | Whether the exchange is a success. |
| `$.content` | Exchange content (equivalent to the response body). |
| `$.attributes` | Exchange attributes. |

**SSE exchange**

| Path | Description |
| --- | --- |
| `$.eventId` | Event id. |
| `$.eventType` | Event type. |
| `$.success` | Always `true` for received events. |
| `$.requestUrl` | URL of the SSE endpoint. |
| `$.requestMethod` | HTTP method used. |
| `$.content` | Event data. |
| `$.attributes` | Event attributes. |

## jsonpath

```
${jsonpath:$.request.url}
${jsonpath:<expression>:'<separator>'}
```

Evaluates a [JSONPath](https://github.com/json-path/JsonPath) expression against the exchange
root. When the result is a list, items are joined with `, ` by default, or with the separator
given as a trailing quoted string: `${jsonpath:$.response.headers.Set-Cookie:' | '}`.

## jmespath

```
${jmespath:<expression>}
```

Evaluates a [JMESPath](https://jmespath.org/) expression for filtering and transforming JSON.

## xpath

```
${xpath:<expression>}
```

Evaluates an XPath expression against XML content in the request or response.

## regex

```
${regex:<pattern>}
${regex:<pattern>:<input>}
```

Applies a Java regular expression. Without `<input>`, the pattern runs against the exchange
content. With `<input>`, the pattern runs against a reference that starts with `$`, `body`,
`attributes`, `request.`, or `response.`. The result is capture group 1 if the pattern has
groups, otherwise the whole match. Colons inside the pattern must be escaped as `\:`.

```
${regex:id=([0-9]+):body}
```

## header, param, cookie

```
${header:<name>}
${param:<name>}
${cookie:<name>}
```

Extract a header, a query parameter, or a cookie from the request or response.

## datetime

```
${datetime:now:<format>}
${datetime:current:<format>}
${datetime:moment:<format>}
${datetime:epoch}
${datetime:epoch:<format>}
${datetime:<source>}
${datetime:<source>:<format>}
```

- `now` / `current` — current time.
- `moment` — the exchange timestamp.
- `epoch` — current time in milliseconds (default), or formatted with `<format>`.
- `<source>` — an epoch-millis string or an ISO date-time string.

`<format>` is a Java `DateTimeFormatter` pattern, e.g. `yyyy-MM-dd HH:mm:ss`. If omitted, the
ISO-8601 format in UTC is used.

## if

```
${if:<condition>:<trueValue>:<falseValue>}
```

Returns `<trueValue>` when the condition holds, `<falseValue>` otherwise. Supported
conditions:

| Condition | Meaning |
| --- | --- |
| `true` / `false` | Constant. |
| `status:<code>` | Status equals `<code>`. |
| `status:>=<code>`, `status:><code>`, `status:<=<code>`, `status:<<code>` | Status comparison. |
| `status:<min>-<max>` | Status within an inclusive range. |
| `has:<attribute>` | Attribute exists and is non-empty. |
| `contains:<attribute>:<text>` | Attribute contains `<text>`. |
| `matches:<attribute>:<regex>` | Attribute matches a regex. |
| `<attribute><op><value>` | Numeric comparison (`>`, `<`, `>=`, `<=`, `==`, `!=`). |

```
${if:status:200:ok:ko}
${if:status:>=400:failed:ok}
${if:has:myAttribute:present:absent}
```

> The processor's canonical name is `if`. `conditional` is accepted as a validation alias, but
> the placeholder must be written `${if:...}`.

## hash

```
${hash:<algorithm>:<text>}
```

Returns the hexadecimal hash of the literal `<text>`. Supported algorithms: `MD5`, `SHA-1`,
`SHA-256`, `SHA-384`, `SHA-512`, `SHA3-256`, `SHA3-384`, `SHA3-512`.

## math

```
${math:<expression>}
```

Evaluates a mathematical expression, including parentheses, with the usual operator
precedence.

```
${math:(1 + 2) * 3}
```

## random

```
${random.<type>}
${random.<type>:<min>}
${random.<type>:<min>:<max>}
${random.string:<length>}
```

| Type | Result | Defaults |
| --- | --- | --- |
| `int`, `integer` | Integer in `[min, max]` | min `0`, max `100` |
| `long` | Long in `[min, max]` | min `0`, max `1000` |
| `double`, `float` | Floating-point value in `[min, max)` | min `0.0`, max `1.0` |
| `uuid` | Random UUID | — |
| `string`, `str` | Random alphanumeric string of `<length>` | length `10` |
| `boolean`, `bool` | Random boolean | — |

```
${random.uuid}
${random.int:1:100}
${random.string:16}
```

## Debugging

Enable debug logging for the template engine to see parsing and evaluation details:

```properties
log4j.logger.io.github.clescot.core.http.template=DEBUG
```

A processor that fails returns the original template text unchanged and logs a warning.
