# Message formats

The connectors exchange Kafka Connect `Struct` values. This page describes the fields of each
type and links to the JSON Schema artifacts shipped with the connectors.

## Converters and Struct values

Kafka Connect `Converters` turn a byte array into a value plus a `Schema`. JSON Schema, Avro,
and Protobuf converters produce a `Struct`; the `StringConverter` produces a `String`.

The HTTP Sink connector accepts:

- **structured messages** (`Struct`) from JSON Schema, Avro, or Protobuf — JSON Schema is the
  tested path and the main use case;
- **String messages** (or `byte[]`) — the `DIRECT` mapper parses them as a JSON `HttpRequest`.

When you produce a `String`, the value is parsed into an `HttpRequest` with Jackson, so its
field names must match the ones below.

## HttpRequest

The incoming message of the HTTP Sink connector.

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `url` | string | yes | Target URL. |
| `method` | string | no | Defaults to `GET`. |
| `headers` | map of string → array of string | no | `X-Request-ID` and `X-Correlation-ID` are used for request tracking. |
| `bodyType` | string | no | `STRING`, `FORM`, `BYTE_ARRAY`, or `MULTIPART`. Default `STRING`. |
| `bodyAsString` | string | no | Body when `bodyType` is `STRING`. |
| `bodyAsForm` | map of string → string | no | Body when `bodyType` is `FORM`. |
| `bodyAsByteArray` | string | no | Body when `bodyType` is `BYTE_ARRAY`. |
| `bodyAsMultipart` | array of string | no | Body when `bodyType` is `MULTIPART`. |

The sink can generate a missing `X-Request-ID` or `X-Correlation-ID`; see
[HTTP request enrichment](http-sink-connector.md#http-request-enrichment).

## HttpResponse

| Field | Type | Required |
| --- | --- | --- |
| `statusCode` | integer | yes |
| `statusMessage` | string | yes |
| `protocol` | string | no |
| `responseHeaders` | map of string → array of string | no |
| `responseBody` | string | no |

## HttpExchange

The value published by the HTTP Sink connector.

| Field | Type | Notes |
| --- | --- | --- |
| `durationInMillis` | long | Exchange duration. |
| `moment` | string | Exchange timestamp. |
| `attempts` | integer | Number of attempts. |
| `success` | boolean | Whether the exchange is a success. |
| `httpRequest` | `HttpRequest` | The request. |
| `httpResponse` | `HttpResponse` | The response. |

## JSON Schema artifacts

The connectors ship JSON Schema definitions under
`kafka-connect-http-connectors/src/main/resources/schemas/json/versions/`:

- `1/`, `2/`, `3/` — versioned schemas;
- each version provides `http-request.json`, `http-response.json`, and `http-exchange.json`
  (versions 2 and 3 also ship `http-part.json`).

Use the version that matches your schema registry setup.
