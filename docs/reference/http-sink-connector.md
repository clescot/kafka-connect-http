# HTTP Sink connector reference

Reference for `io.github.clescot.kafka.connect.http.sink.HttpSinkConnector`. Every setting is
listed with its type and default. Values are the ones defined by the connector's
configuration definitions in `http-client` and `kafka-connect-http-connectors`.

## Required settings

| Setting | Description |
| --- | --- |
| `connector.class` | `io.github.clescot.kafka.connect.http.sink.HttpSinkConnector` |
| `topics` or `topics.regex` | Topic(s) the connector reads HTTP request messages from. |

## Common Kafka Connect settings

| Setting | Default | Description |
| --- | --- | --- |
| `tasks.max` | `1` | Maximum number of tasks. |
| `key.converter` / `value.converter` | — | Standard Kafka Connect converters. |
| `errors.*` | — | Standard error handling and dead-letter-queue settings. |

## Publish settings

| Setting | Default | Description |
| --- | --- | --- |
| `publish.mode` | `NONE` | `NONE` or `PRODUCER`. See [choose a publish mode](../how-to/choose-a-publish-mode.md). |

## Producer settings

Used only with `publish.mode: PRODUCER`. The connector uses its own low-level Kafka producer;
every `producer.*` setting is passed to it.

### Producer connection and topics

| Setting | Default | Description |
| --- | --- | --- |
| `producer.bootstrap.servers` | `""` | Broker addresses. Required to reach the cluster. |
| `producer.success.topic` | `http-success` | Topic for successful exchanges. |
| `producer.error.topic` | `http-errors` | Topic for failed exchanges. |
| `producer.dlq.topic` | `http-dlq` | Topic for records whose retry delay is too long. |
| `producer.content` | `exchange` | `exchange` (full `HttpExchange`) or `response` (only the `HttpResponse`). |
| `producer.format` | `string` | `string` or `json`. |

### Producer JSON format

| Setting | Default | Description |
| --- | --- | --- |
| `producer.json.indent.output` | `false` | Indent the JSON output. |
| `producer.json.write.dates.as.iso.8601` | `true` | Write dates in ISO-8601. |
| `producer.json.schema.spec.version` | `draft_2019_09` | JSON Schema spec version. |
| `producer.json.one.of.for.nullables` | `true` | Represent nullables with `oneOf`. |
| `producer.json.fail.invalid.schema` | `true` | Fail on an invalid schema. |
| `producer.json.fail.unknown.properties` | `true` | Fail on unknown properties. |
| `producer.json.key.subject.name.strategy` | `io.confluent.kafka.serializers.subject.TopicRecordNameStrategy` | Subject name strategy for keys. |
| `producer.json.value.subject.name.strategy` | `io.confluent.kafka.serializers.subject.TopicRecordNameStrategy` | Subject name strategy for values. |

### Producer schema registry

| Setting | Default | Description |
| --- | --- | --- |
| `producer.schema.registry.url` | `""` | Schema registry URL and port. |
| `producer.schema.registry.cache.capacity` | `1000` | Cache capacity. |
| `producer.schema.registry.auto.register` | `true` | Auto-register schemas. |
| `producer.missing.id.cache.ttl.sec` | — | `CachedSchemaRegistryClient` cache TTL. |
| `producer.missing.version.cache.ttl.sec` | — | `CachedSchemaRegistryClient` cache TTL. |
| `producer.missing.schema.cache.ttl.sec` | — | `CachedSchemaRegistryClient` cache TTL. |
| `producer.missing.cache.size` | — | `CachedSchemaRegistryClient` cache size. |
| `producer.bearer.auth.cache.expiry.buffer.seconds` | — | Bearer auth cache expiry buffer. |
| `producer.bearer.auth.scope.claim.name` | — | Bearer auth scope claim name. |
| `producer.bearer.auth.sub.claim.name` | — | Bearer auth subject claim name. |

Any other `producer.*` key is forwarded to the producer instance.

## Message splitters

Split one message value into several messages. No splitter is registered by default.

| Setting | Description |
| --- | --- |
| `message.splitter.ids` | Comma-separated list of splitter ids to register. |
| `message.splitter.<id>.pattern` | Java regex used to split the body. |
| `message.splitter.<id>.limit` | Result threshold, like `String.split`. |
| `message.splitter.<id>.matcher` | JEXL expression deciding which messages to split. |

## HttpRequestMapper

An `HttpRequestMapper` turns a message into an `HttpRequest`. A `default` mapper is always
registered and catches every message.

| Setting | Default | Description |
| --- | --- | --- |
| `http.request.mapper.ids` | `[]` | Extra mapper ids to register (comma-separated). |
| `http.request.mapper.<id>.mode` | `DIRECT` | `DIRECT` or `JEXL`. |
| `http.request.mapper.<id>.matcher` | `true` | JEXL expression tested against the `sinkRecord`. |
| `http.request.mapper.<id>.url` | — | JEXL expression for the request URL. **Required** in `JEXL` mode. |
| `http.request.mapper.<id>.method` | — | JEXL expression for the HTTP method. |
| `http.request.mapper.<id>.bodytype` | — | JEXL expression for the body type. |
| `http.request.mapper.<id>.body` | — | JEXL expression for the body. |
| `http.request.mapper.<id>.headers` | — | JEXL expression for the headers, e.g. `{'name':['v1','v2']}`. |
| `http.request.mapper.<id>.split.pattern` | — | Regex to split a String body into several requests. |
| `http.request.mapper.<id>.split.limit` | `0` | Number of times the split pattern is applied. |

- **`DIRECT`** — the message value is already an `HttpRequest`: a `Struct`, or a JSON string
  parsed into one.
- **`JEXL`** — the `HttpRequest` is built from fields extracted with JEXL expressions
  evaluated against the variable `sinkRecord`.

## Request groupers

Regroup several requests into one request whose body contains the grouped content. No grouper
is registered by default.

| Setting | Description |
| --- | --- |
| `request.grouper.ids` | Comma-separated list of grouper ids to register. |
| `request.grouper.<id>.predicate.url.regex` | URL matcher. |
| `request.grouper.<id>.predicate.method.regex` | Method matcher. |
| `request.grouper.<id>.predicate.bodytype.regex` | Body type matcher. |
| `request.grouper.<id>.predicate.header.key.regex` | Header key matcher. |
| `request.grouper.<id>.predicate.header.value.regex` | Header value matcher. |
| `request.grouper.<id>.separator` | String inserted between grouped bodies. |
| `request.grouper.<id>.start` | String prepended to the grouped body. |
| `request.grouper.<id>.end` | String appended to the grouped body. |
| `request.grouper.<id>.message.limit` | Maximum number of messages grouped into one. |
| `request.grouper.<id>.body.limit` | Maximum grouped body length, in bytes. |

## Metrics settings

Metrics registry is JVM-global. See [metrics](metrics.md).

| Setting | Default | Description |
| --- | --- | --- |
| `meter.registry.exporter.jmx.activate` | `false` | Export via JMX. |
| `meter.registry.exporter.prometheus.activate` | `false` | Export via Prometheus. |
| `meter.registry.exporter.prometheus.port` | `9090` | Prometheus exposition port. |
| `meter.registry.bind.metrics.executor.service` | `false` | Bind executor service metrics. |
| `meter.registry.bind.metrics.jvm.memory` | `false` | Bind JVM memory metrics. |
| `meter.registry.bind.metrics.jvm.thread` | `false` | Bind JVM thread metrics. |
| `meter.registry.bind.metrics.jvm.info` | `false` | Bind JVM info metrics. |
| `meter.registry.bind.metrics.jvm.gc` | `false` | Bind JVM GC metrics. |
| `meter.registry.bind.metrics.jvm.classloader` | `false` | Bind JVM classloader metrics. |
| `meter.registry.bind.metrics.jvm.processor` | `false` | Bind JVM processor metrics. |
| `meter.registry.bind.metrics.logback` | `false` | Bind Logback metrics. |
| `meter.registry.tag.include.legacy.host` | `false` | Add the legacy `host` tag (alias of `target.host`). |
| `meter.registry.tag.include.url.path` | `false` | Add the `target.uri` tag with the URL path. High cardinality risk. |

## Async execution

| Setting | Default | Description |
| --- | --- | --- |
| `httpclient.async.fixed.thread.pool.size` | — | Dedicated fixed thread pool size for async requests. If unset, the common ForkJoin pool is used. |

## Configuration settings

Settings that live under a configuration prefix `config.<id>.`. A **configuration** groups the
HTTP client behaviour that applies to a set of requests. The `default` configuration always
exists (`config.default.`). Register more with:

| Setting | Default | Description |
| --- | --- | --- |
| `config.ids` | `[]` | Comma-separated ids of custom configurations. Do not list `default`. |

### Predicate

Selects the requests a configuration applies to. All matchers are combined with a logical
AND. Requests matching no configuration use `default`.

| Setting | Description |
| --- | --- |
| `config.<id>.predicate.url.regex` | URL regex. |
| `config.<id>.predicate.method.regex` | HTTP method regex. |
| `config.<id>.predicate.bodytype.regex` | Body type regex. |
| `config.<id>.predicate.header.key.regex` | Header key regex. |
| `config.<id>.predicate.header.value.regex` | Header value regex (only with a header key). |

### HTTP request enrichment

| Setting | Default | Description |
| --- | --- | --- |
| `config.<id>.enrich.request.static.header.names` | `[]` | Header names to add to every request. Each name is looked up in `config.<id>.enrich.request.static.header.<name>`. |
| `config.<id>.enrich.request.generate.missing.correlation.id` | `false` | Generate an `X-Correlation-ID` if missing. |
| `config.<id>.enrich.request.generate.missing.request.id` | `false` | Generate an `X-Request-ID` if missing. |
| `config.<id>.enrich.request.useragent.override.with` | `http_client` | `http_client`, `project`, or `custom`. |
| `config.<id>.enrich.request.useragent.custom.values` | — | Custom User-Agent value(s), `|`-separated; one is picked at random per query. |

With `override.with: project`, the User-Agent is
`Mozilla/5.0 (compatible;kafka-connect-http/<version>; okhttp; https://github.com/clescot/kafka-connect-http)`.

### Success and retry decisions

| Setting | Default | Description |
| --- | --- | --- |
| `config.<id>.enrich.exchange.success.response.code.regex` | `^[1-2][0-9][0-9]$` | Status codes considered a success. |
| `config.<id>.retry.policy.response.code.regex` | `^5[0-9][0-9]$` | Status codes that trigger a retry. |
| `config.<id>.retry.policy.retries` | `1` | Number of retries before failing. |
| `config.<id>.retry.policy.retry.delay.in.ms` | `2000` | Initial delay before the first retry. |
| `config.<id>.retry.policy.retry.max.delay.in.ms` | `20000` | Maximum delay between retries. |
| `config.<id>.retry.policy.retry.delay.factor` | `1.5` | Growth factor applied to the delay. |
| `config.<id>.retry.policy.retry.jitter.in.ms` | `500` | Maximum jitter added to the delay. |

### Retry-After handling

| Setting | Default | Description |
| --- | --- | --- |
| `config.<id>.retry.policy.retry.after.status.code` | `503|429|301` | Status codes whose `Retry-After` header is honoured. |
| `config.<id>.retry.policy.retry.after.max.duration.in.sec` | `86400` | Maximum `Retry-After` duration considered. |
| `config.<id>.retry.policy.retry.after.max.threshold.in.sec` | `60` | Above this delay the circuit breaker opens; below it the connector waits locally. |
| `config.<id>.retry.policy.retry.after.delay.in.sec` | `3600` | Delay used when no `Retry-After` value is present. |

### Server-advertised rate limits

The connector honours the IETF `RateLimit` / `RateLimit-Policy` response header fields
([draft-ietf-httpapi-ratelimit-headers](https://datatracker.ietf.org/doc/draft-ietf-httpapi-ratelimit-headers/)).
When a response advertises an exhausted service limit (`r=0` on a `requests` or
`concurrent-requests` quota), later calls to the same target authority wait for the advertised
window before being sent. A window above the threshold opens the circuit breaker instead.

| Setting | Default | Description |
| --- | --- | --- |
| `config.<id>.rate.limit.headers.enabled` | `true` | Honour the `RateLimit` fields at all. |
| `config.<id>.rate.limit.headers.scope` | `instance` | `instance` (per configuration in this connector) or `static` (one gate per configuration id shared across connector instances in the JVM). |
| `config.<id>.rate.limit.headers.max.threshold.in.sec` | `60` | Above this window the circuit breaker opens; below it the connector waits locally. |
| `config.<id>.rate.limit.headers.max.duration.in.sec` | `86400` | Maximum advertised window considered; longer values are capped. |
| `config.<id>.rate.limit.headers.default.delay.in.sec` | `3600` | Delay used when an exhausted service limit has neither `t` nor a matching policy `w`. |

`Retry-After` / `X-Retry-After` takes precedence when both are present (the draft requires it).
Malformed fields, items without `r`, and non-admission quota units (`content-bytes`, unknown
units) are ignored; the pause is jittered by up to 500 ms on resume.

### Rate limiter

| Setting | Default | Description |
| --- | --- | --- |
| `config.<id>.rate.limiter.period.in.ms` | `1000` | Period over which executions are capped. |
| `config.<id>.rate.limiter.max.executions` | `1` | Maximum executions per period. |
| `config.<id>.rate.limiter.scope` | `instance` | `instance` (per configuration in this connector) or `static` (shared across connector instances in the JVM). |
| `config.<id>.rate.limiter.permits.per.execution` | `one` | `one` per call, or `request_length` (body + headers size in bytes). |

### Response limits

| Setting | Default | Description |
| --- | --- | --- |
| `config.<id>.http.response.message.status.limit` | `1024` | Truncate the status message to this length. |
| `config.<id>.http.response.headers.limit` | `10000` | Truncate response headers to this length. |
| `config.<id>.http.response.body.limit` | `100000` | Truncate the body to this length. |

### Exchange templating

| Setting | Default | Description |
| --- | --- | --- |
| `config.<id>.exchange.template` | `""` | Template applied before publishing. Empty disables templating. |
| `config.<id>.exchange.template.processors` | `""` | Comma-separated allow-list of processors. Empty allows all built-ins. |

See [customize exchange templates](../how-to/customize-exchange-templates.md) and
[exchange template syntax](exchange-template-syntax.md).

### HTTP client

| Setting | Default | Description |
| --- | --- | --- |
| `config.<id>.httpclient.implementation` | `okhttp` | `okhttp` or `ahc` (Async HTTP Client). |
| `config.<id>.httpclient.cookie.policy` | `ACCEPT_ALL` | `ACCEPT_NONE`, `ACCEPT_ORIGINAL_SERVER`, or `ACCEPT_ALL`. |
| `config.<id>.httpclient.secure.random.activate` | `false` | Use a secure random generator. |
| `config.<id>.httpclient.secure.random.prng.algorithm` | `SHA1PRNG` | PRNG algorithm when the secure random is active. |
| `config.<id>.httpclient.unsecure.random.seed` | — | Seed when the secure random is inactive. |

#### SSL

| Setting | Default | Description |
| --- | --- | --- |
| `config.<id>.httpclient.ssl.keystore.path` / `.password` / `.type` / `.algorithm` | — | Client key material. Types: `jks` or `pkcs12`. |
| `config.<id>.httpclient.ssl.truststore.path` / `.password` / `.type` / `.algorithm` | — | Trusted server certificates. |
| `config.<id>.httpclient.ssl.truststore.always.trust` | `false` | Trust any certificate. Disables transport security. |

#### Authentication

| Setting | Default | Description |
| --- | --- | --- |
| `config.<id>.httpclient.authentication.basic.activate` | `false` | Activate Basic authentication. |
| `config.<id>.httpclient.authentication.basic.username` / `.password` | — | Basic credentials. |
| `config.<id>.httpclient.authentication.basic.charset` | `ISO-8859-1` | Charset for the credentials. |
| `config.<id>.httpclient.authentication.digest.activate` | `false` | Activate Digest authentication. |
| `config.<id>.httpclient.authentication.digest.username` / `.password` | — | Digest credentials. |
| `config.<id>.httpclient.authentication.digest.charset` | `US-ASCII` | Charset for the credentials. |
| `config.<id>.httpclient.authentication.oauth2.client.credentials.flow.activate` | `false` | Activate OAuth2 client credentials. |
| `config.<id>.httpclient.authentication.oauth2.client.credentials.flow.well.known.url` | — | Well-known configuration endpoint. |
| `config.<id>.httpclient.authentication.oauth2.client.credentials.flow.client.authentication.method` | `client_secret_basic` | `client_secret_basic`, `client_secret_post`, or `client_secret_jwt`. |
| `config.<id>.httpclient.authentication.oauth2.client.credentials.flow.client.id` / `.secret` | — | OAuth2 client credentials. |
| `config.<id>.httpclient.authentication.oauth2.client.credentials.flow.client.issuer` | — | Issuer, for JWT. |
| `config.<id>.httpclient.authentication.oauth2.client.credentials.flow.client.jws.algorithm` | `HS256` | JWS algorithm, for JWT. |
| `config.<id>.httpclient.authentication.oauth2.client.credentials.flow.scopes` | — | Optional scopes, comma-separated. |

See [configure authentication](../how-to/configure-authentication.md).

#### Proxy

| Setting | Default | Description |
| --- | --- | --- |
| `config.<id>.proxy.httpclient.hostname` | — | Proxy hostname. |
| `config.<id>.proxy.httpclient.port` | — | Proxy port. |
| `config.<id>.proxy.httpclient.type` | `HTTP` | `HTTP`, `DIRECT`, or `SOCKS`. |
| `config.<id>.httpclient.proxy.authentication.basic.activate` | `false` | Activate Basic proxy authentication. |
| `config.<id>.httpclient.proxy.authentication.basic.username` / `.password` | — | Proxy credentials. |
| `config.<id>.httpclient.proxy.authentication.basic.charset` | `ISO-8859-1` | Charset. |
| `config.<id>.httpclient.proxy.authentication.digest.activate` | `false` | Activate Digest proxy authentication. |
| `config.<id>.httpclient.proxy.authentication.digest.username` / `.password` | — | Proxy credentials. |
| `config.<id>.httpclient.proxy.authentication.digest.charset` | `US-ASCII` | Charset. |

#### Proxy selector

`<index>` is an increasing integer starting at `0`.

| Setting | Default | Description |
| --- | --- | --- |
| `config.<id>.proxyselector.httpclient.algorithm` | `uriregex` | `uriregex`, `random`, `weightedrandom`, or `hosthash`. |
| `config.<id>.proxyselector.httpclient.<index>.hostname` | — | Proxy hostname. |
| `config.<id>.proxyselector.httpclient.<index>.port` | — | Proxy port. |
| `config.<id>.proxyselector.httpclient.<index>.type` | `HTTP` | `HTTP`, `DIRECT`, or `SOCKS`. |
| `config.<id>.proxyselector.httpclient.<index>.uri.regex` | — | URI matcher, for `uriregex`. |
| `config.<id>.proxyselector.httpclient.<index>.weight` | — | Weight, for `weightedrandom`. |
| `config.<id>.proxyselector.httpclient.non.proxy.hosts.uri.regex` | — | Hosts reached without a proxy. |

#### okhttp client (default)

| Setting | Default | Description |
| --- | --- | --- |
| `config.<id>.okhttp.call.timeout` | `0` | Timeout for a complete call, ms. `0` = no timeout. |
| `config.<id>.okhttp.connect.timeout` | `0` | Connection timeout, ms. |
| `config.<id>.okhttp.read.timeout` | `0` | Read timeout, ms. |
| `config.<id>.okhttp.write.timeout` | `0` | Write timeout, ms. |
| `config.<id>.okhttp.protocols` | — | Preferred protocols, comma-separated (`HTTP_1_1`, `HTTP_2`, `H2_PRIOR_KNOWLEDGE`, `QUIC`). |
| `config.<id>.okhttp.follow.redirect` | `true` | Follow redirects. |
| `config.<id>.okhttp.follow.ssl.redirect` | `true` | Follow SSL redirects. |
| `config.<id>.okhttp.retry.on.connection.failure` | `true` | Retry a failed connection attempt. |
| `config.<id>.okhttp.ssl.skip.hostname.verification` | `false` | Skip hostname verification (unsafe). |
| `config.<id>.okhttp.connection.pool.scope` | `instance` | `instance` or `static`. |
| `config.<id>.okhttp.connection.pool.max.idle.connections` | `0` | Idle connections kept in the pool. `0` disables the pool. |
| `config.<id>.okhttp.connection.pool.keep.alive.duration` | `0` | Keep-alive duration in the pool, ms. |
| `config.<id>.okhttp.dispatcher.max.requests` | `64` | Maximum concurrent requests. |
| `config.<id>.okhttp.dispatcher.max.requests.per.host` | `5` | Maximum concurrent requests per host. |
| `config.<id>.okhttp.cache.activate` | `false` | Activate the response cache. |
| `config.<id>.okhttp.cache.max.size` | `0` | Maximum number of cache entries. |
| `config.<id>.okhttp.cache.type` | `file` | `file` or `inmemory`. |
| `config.<id>.okhttp.cache.directory.path` | — | Cache directory. |
| `config.<id>.okhttp.interceptor.logging.activate` | `true` | Log requests and responses at DEBUG. |
| `config.<id>.okhttp.interceptor.inet.address.activate` | `false` | Add `X-Host-Address`, `X-Host-Name`, `X-Canonical-Host-Name` response headers. |
| `config.<id>.okhttp.interceptor.ssl.handshake.activate` | `false` | Log SSL handshake details at DEBUG. |

#### okhttp DNS over HTTPS

| Setting | Default | Description |
| --- | --- | --- |
| `config.<id>.okhttp.doh.activate` | `false` | Resolve DNS over HTTPS. |
| `config.<id>.okhttp.doh.bootstrap.dns.hosts` | — | Hosts used to resolve the DoH URL. System DNS if unset. |
| `config.<id>.okhttp.doh.url` | — | DoH resolver URL. |
| `config.<id>.okhttp.doh.include.ipv6` | `true` | Include IPv6 addresses. |
| `config.<id>.okhttp.doh.use.post.method` | `false` | Use `POST` instead of `GET`. |
| `config.<id>.okhttp.doh.resolve.private.addresses` | `false` | Resolve private addresses. |
| `config.<id>.okhttp.doh.resolve.public.addresses` | `true` | Resolve public addresses. |

#### Async HTTP Client (`httpclient.implementation: ahc`)

| Setting | Default |
| --- | --- |
| `org.asynchttpclient.http.max.connections` | `3` |
| `org.asynchttpclient.http.rate.limit.per.second` | `3` |
| `org.asynchttpclient.http.max.wait.ms` | `500` |
| `org.asynchttpclient.keep.alive.class` | `org.asynchttpclient.channel.DefaultKeepAliveStrategy` |
| `org.asynchttpclient.response.body.part.factory` | `EAGER` |
| `org.asynchttpclient.connection.semaphore.factory` | `org.asynchttpclient.netty.channel.DefaultConnectionSemaphoreFactory` |
| `org.asynchttpclient.cookie.store` | `org.asynchttpclient.cookie.ThreadSafeCookieStore` |
| `org.asynchttpclient.netty.timer` | `io.netty.util.HashedWheelTimer` |
| `org.asynchttpclient.byte.buffer.allocator` | `io.netty.buffer.PooledByteBufAllocator` |

## Known inconsistencies

The following are genuine mismatches between the setting definitions and their documented
text. They are listed so you can confirm the effective behaviour against your build:

- `meter.registry.tag.include.url.path` shares the description of
  `meter.registry.tag.include.legacy.host` in the source.
- `config.<id>.httpclient.cookie.policy` and the connection-pool/rate-limiter `scope`
  settings are typed inconsistently between the `ConfigDef` and their documentation; the
  string values documented above are the intended ones.
- The connection-pool `scope` and dispatcher settings are registered with numeric defaults
  in the `ConfigDef` even though the documented values are symbolic.
