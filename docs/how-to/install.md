# Install the connectors

This guide shows you how to install the kafka-connect-http plugins into a Kafka Connect
cluster and verify that they are loaded.

## Before you start

- A Kafka Connect cluster (or a custom image based on a Kafka Connect image such as
  `confluentinc/cp-kafka-connect`).
- The ability to rebuild that image, or to add jars to a worker's plugin path.
- `confluent-hub` if you install from the released archive (it ships with the
  `confluentinc/cp-kafka-connect` image).

The connectors require **Java 17** and a Kafka Connect runtime in the 4.x line. See the
`pom.xml` for the exact Kafka version currently targeted.

## 1. Get the archive

Each release publishes one Confluent Hub archive that bundles the sink and all source
connectors. Pick either source:

- **GitHub releases** — download `clescot-kafka-connect-http-connectors-<version>.zip` from
  the [releases page](https://github.com/clescot/kafka-connect-http/releases). Replace
  `<version>` with the release you want.
- **Maven Central** — download `kafka-connect-http-connectors-<version>.zip` from
  `https://repo1.maven.org/maven2/io/github/clescot/kafka-connect-http-connectors/<version>/`.

Both contain the same plugins. The connectors are not published on the Confluent Hub
website, so install them offline.

## 2. Install into your Kafka Connect image

Add the archive to your image and run the offline `confluent-hub` install:

```dockerfile
FROM confluentinc/cp-kafka-connect:8.0.0

# Copy the released archive
COPY clescot-kafka-connect-http-connectors-<version>.zip /tmp/kafka-connect-http.zip

# Install it into the Connect plugin path
RUN confluent-hub install /tmp/kafka-connect-http.zip --no-prompt
```

The repository ships a working example at
[`kafka-connect-http-connectors/src/main/resources/Dockerfile`](https://github.com/clescot/kafka-connect-http/blob/master/kafka-connect-http-connectors/src/main/resources/Dockerfile).

> **Why one archive?** Every connector ships in the same archive, so one install gives you the
> HTTP Sink, the Cron Source, and the SSE Source connectors.

## 3. Verify the installation

Start a worker and query the connector plugins REST API:

```bash
curl -s http://localhost:8083/connector-plugins | jq '.[].class' | grep clescot
```

You should see the three connector classes:

```
"io.github.clescot.kafka.connect.http.sink.HttpSinkConnector"
"io.github.clescot.kafka.connect.http.source.cron.CronSourceConnector"
"io.github.clescot.kafka.connect.sse.client.okhttp.SseSourceConnector"
```

If a class is missing, confirm the archive was installed into a directory that is on the
worker's `plugin.path`, then restart the worker.

## Next steps

- Follow the [tutorial](../tutorials/first-http-exchange.md) to send your first request.
- Or jump to the [quick start configurations](quick-start.md).
