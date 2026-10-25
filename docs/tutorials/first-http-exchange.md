# Tutorial: your first HTTP exchange

In this tutorial you will deploy an HTTP Sink connector, send one Kafka message, watch it
become an HTTP request, and read the server's response back from a Kafka topic.

It takes about ten minutes. Follow the steps in order.

## What you will build

```
http-request  ──▶  HTTP Sink Connector  ──▶  HTTP server
                                               │
http-success  ◀──   (response published)  ◀────┘
```

The connector uses `publish.mode: PRODUCER`, so a single sink connector is enough: it calls
the HTTP server and publishes the response itself. No source connector is involved.

## Before you start

You need:

- The connectors [installed](../how-to/install.md) in a running Kafka Connect cluster.
- The Kafka Connect REST API reachable (we use `http://localhost:8083`).
- A way to create topics and produce/consume messages (we use the `kafka-topics` and
  `kafka-console-producer`/`kafka-console-consumer` tools).
- An HTTP endpoint you can call. This tutorial uses `https://httpbin.org/anything`, a public
  echo service that returns whatever you send. Substitute your own endpoint if you prefer.

## Step 1 — Create the topics

```bash
kafka-topics --bootstrap-server localhost:9092 --create --topic http-request
kafka-topics --bootstrap-server localhost:9092 --create --topic http-success
kafka-topics --bootstrap-server localhost:9092 --create --topic http-error
```

## Step 2 — Deploy the sink connector

Save this configuration as `sink.json`:

```json
{
  "name": "my-first-http-sink",
  "config": {
    "connector.class": "io.github.clescot.kafka.connect.http.sink.HttpSinkConnector",
    "tasks.max": "1",
    "topics": "http-request",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "publish.mode": "PRODUCER",
    "producer.format": "string",
    "producer.bootstrap.servers": "localhost:9092",
    "producer.success.topic": "http-success",
    "producer.error.topic": "http-error"
  }
}
```

Create it:

```bash
curl -X PUT -H "Content-Type: application/json" \
  --data @sink.json \
  http://localhost:8083/connectors/my-first-http-sink/config
```

The connector should start with no error. The default `HttpRequestMapper` (`DIRECT`) turns
each message whose value is a JSON object into an `HttpRequest`.

## Step 3 — Send one request message

The message value is a JSON `HttpRequest`. You only need a `url`; everything else has
defaults.

Start the producer:

```bash
kafka-console-producer --bootstrap-server localhost:9092 --topic http-request
```

Then paste this single line and press Enter:

```json
{"url":"https://httpbin.org/anything","method":"POST","bodyAsString":"hello from kafka-connect-http"}
```

## Step 4 — Read the response

Start a consumer on the success topic in another terminal:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic http-success --from-beginning
```

You should see a message describing the exchange: the request you sent and the response
`httpbin` returned (with status code `200`). The connector treated it as a success because
the default success regex, `^[1-2][0-9][0-9]$`, matches `200`.

If the topic stays empty, check `http-error` instead, and see [troubleshoot](../how-to/troubleshoot.md).

## What just happened

1. The sink connector read your message from `http-request`.
2. The `DIRECT` mapper parsed the JSON value into an `HttpRequest`.
3. The HTTP client (`okhttp` by default) executed the request.
4. The response was wrapped in an `HttpExchange` and, because the status code was a success,
   published to `http-success` by the connector's own producer.

## Clean up

```bash
curl -X DELETE http://localhost:8083/connectors/my-first-http-sink
```

## Next steps

Now that you have a working exchange, you can:

- Choose what happens to responses: [choose a publish mode](../how-to/choose-a-publish-mode.md).
- Send requests on a schedule: [quick start](../how-to/quick-start.md#generate-requests-on-a-schedule).
- Reshape the published data: [customize exchange templates](../how-to/customize-exchange-templates.md).
- Understand how the connector decides success vs failure:
  [request handling](../explanation/request-handling.md).
