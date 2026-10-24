# Metrics

The HTTP client can expose metrics through a meter registry. The registry is **global to the
JVM**, so the settings below configure it once per Kafka Connect worker, even though they are
declared on a connector.

Export is opt-in. The JMX and Prometheus exporters can be combined.

| Setting | Default | Description |
| --- | --- | --- |
| `meter.registry.exporter.jmx.activate` | `false` | Export metrics via JMX. |
| `meter.registry.exporter.prometheus.activate` | `false` | Export metrics via Prometheus. |
| `meter.registry.exporter.prometheus.port` | `9090` | Prometheus exposition port. |

## HTTP client timers

When at least one exporter is active and the **okhttp** client is used (the default), a
listener publishes these timers:

`okhttp`, `okhttp.dns`, `okhttp.socket.connection`, `okhttp.pool.connection`,
`okhttp.proxy.select`, `okhttp.request.headers`, `okhttp.request.body`,
`okhttp.response.headers`, `okhttp.response.body`.

> The Async HTTP Client (`ahc`) implementation does not expose these timers.

Each timer carries these tags:

| Tag | Description |
| --- | --- |
| `configuration.id` | The configuration that handled the request. |
| `method` | HTTP method. |
| `outcome` | Response code, or `UNKNOWN`. |
| `status` | Response code, `IO_ERROR`, or `CLIENT_ERROR`. |
| `target.host` | Remote host. |
| `target.port` | Remote port. |
| `target.scheme` | URL scheme. |
| `target.uri` | `none`, unless enabled (see below). |
| `host` | Legacy alias of `target.host`, disabled by default. |

| Setting | Default | Description |
| --- | --- | --- |
| `meter.registry.tag.include.legacy.host` | `false` | Add the legacy `host` tag. |
| `meter.registry.tag.include.url.path` | `false` | Populate `target.uri` with the URL path. Be careful: many distinct paths cause high-cardinality metrics. |

## Built-in metrics

| Setting | Default | Binds |
| --- | --- | --- |
| `meter.registry.bind.metrics.executor.service` | `false` | Executor service metrics. |
| `meter.registry.bind.metrics.jvm.memory` | `false` | JVM memory. |
| `meter.registry.bind.metrics.jvm.thread` | `false` | JVM threads. |
| `meter.registry.bind.metrics.jvm.info` | `false` | JVM info. |
| `meter.registry.bind.metrics.jvm.gc` | `false` | JVM garbage collector. |
| `meter.registry.bind.metrics.jvm.classloader` | `false` | JVM class loaders. |
| `meter.registry.bind.metrics.jvm.processor` | `false` | JVM processors. |
| `meter.registry.bind.metrics.logback` | `false` | Logback. |

## Example

```json
{
  "meter.registry.exporter.jmx.activate": "true",
  "meter.registry.exporter.prometheus.activate": "true",
  "meter.registry.exporter.prometheus.port": "9087",
  "meter.registry.bind.metrics.jvm.memory": "true",
  "meter.registry.bind.metrics.executor.service": "true"
}
```

See the [HTTP Sink reference](http-sink-connector.md#metrics-settings) for where these settings
are declared.
