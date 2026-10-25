# Quick start configurations

Copy-paste configurations for the common setups. This page is a recipe book: each section
solves one specific configuration problem. It assumes the connectors are
[installed](install.md) and that the topics you reference already exist.

The requests are read from the topics configured with `topics` (or `topics.regex`). See
[message formats](../reference/message-formats.md) for the shape of an incoming message.

All examples are created or updated with:

```bash
curl -X PUT -H "Content-Type: application/json" \
  --data @config.json \
  http://my-kafka-connect-cluster:8083/connectors/<connector-name>/config
```

## Send requests and ignore the responses

The simplest configuration. `publish.mode` defaults to `NONE`, so HTTP responses are
discarded and no source connector is needed.

```json
{
  "name": "my-http-sink-connector",
  "config": {
    "connector.class": "io.github.clescot.kafka.connect.http.sink.HttpSinkConnector",
    "tasks.max": "1",
    "topics": "http-request"
  }
}
```

## Send requests and publish responses with a low-level producer

Set `publish.mode` to `PRODUCER`. The connector uses its own Kafka producer, so you must
give it the broker connection details and the target topics. No source connector is
required, and there is no co-location constraint.

String output:

```json
{
  "name": "my-http-sink-connector",
  "config": {
    "connector.class": "io.github.clescot.kafka.connect.http.sink.HttpSinkConnector",
    "tasks.max": "1",
    "topics": "http-request",
    "publish.mode": "PRODUCER",
    "producer.format": "string",
    "producer.bootstrap.servers": "kafka:9092",
    "producer.success.topic": "http-success",
    "producer.error.topic": "http-error"
  }
}
```

JSON output (requires a schema registry):

```json
{
  "name": "my-http-sink-connector",
  "config": {
    "connector.class": "io.github.clescot.kafka.connect.http.sink.HttpSinkConnector",
    "tasks.max": "1",
    "topics": "http-request",
    "publish.mode": "PRODUCER",
    "producer.format": "json",
    "producer.bootstrap.servers": "kafka:9092",
    "producer.schema.registry.url": "https://my-schema-registry:8081",
    "producer.success.topic": "http-success",
    "producer.error.topic": "http-error"
  }
}
```

See the [`producer.*` reference](../reference/http-sink-connector.md#producer-settings) for
every producer setting.

## Generate requests on a schedule

The Cron Source connector emits an HTTP request message on a cron schedule. Point it at the
topic the sink connector listens to.

```json
{
  "name": "my-cron-source-connector",
  "config": {
    "connector.class": "io.github.clescot.kafka.connect.http.source.cron.CronSourceConnector",
    "tasks.max": "1",
    "topic": "http-request",
    "jobs": "job1",
    "job.job1.url": "http://mywebsite.com/ping",
    "job.job1.cron": "0/5 * * ? * *",
    "job.job1.method": "POST",
    "job.job1.body": "stuff",
    "job.job1.headers": "Content-Type,X-Correlation-ID",
    "job.job1.header.Content-Type": "application/json",
    "job.job1.header.X-Correlation-ID": "e6de70d1-f222-46e8-b755-754880687822"
  }
}
```

The cron expression uses
[Quartz syntax](https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html).

## Consume Server-Sent Events

The SSE Source connector connects to one or more SSE endpoints and publishes each event to a
topic. Each endpoint is a "configuration" identified by an id listed in `config.ids`.

```json
{
  "name": "my-sse-source-connector",
  "config": {
    "connector.class": "io.github.clescot.kafka.connect.sse.client.okhttp.SseSourceConnector",
    "tasks.max": "1",
    "config.ids": "default",
    "config.default.url": "http://mywebsite.com/sse",
    "config.default.topic": "sse_events"
  }
}
```

## Next steps

- Decide what to do with responses: [choose a publish mode](choose-a-publish-mode.md).
- Tune the HTTP client, retries, and rate limiting: [`config.<id>.*` reference](../reference/http-sink-connector.md#configuration-settings).
- Rewrite the published data: [customize exchange templates](customize-exchange-templates.md).
