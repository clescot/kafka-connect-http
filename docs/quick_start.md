# Quick start guide

Minimal configuration to interact with HTTP servers.

## Configuration ignoring HTTP results

### Sink Connector configuration

This configuration does NOT publish HTTP results to an in memory queue.

```json 
{
    "name": "my-http-sink-connector",
    "config": {
    "connector.class":"sink.io.github.clescot.kafka.connect.http.HttpSinkConnector",
    "tasks.max": "1",
    "topics":"http-request"
    }
}
```

## Configuration to call web sites and publish results

### Sink Connector configuration

This configuration publish HTTP results to an in memory queue.

```json 
{
    "name": "my-http-sink-connector",
    "config": {
    "connector.class":"sink.io.github.clescot.kafka.connect.http.HttpSinkConnector",
    "tasks.max": "1",
    "topics":"http-request",
    "publish.to.in.memory.queue":"true"
    }
}
```

### Source Connector configuration

This configuration listen to the in memory queue, to publish HTTP results in the configured `success.topic` or `error.topic`topic,
depending on the HTTP result.

```json 
{
    "name": "my-http-source-connector",
    "config": {
    "connector.class":"source.io.github.clescot.kafka.connect.http.HttpSourceConnector",
    "tasks.max": "1",
    "success.topic": "http-success",
    "error.topic": "http-error"
    }
}
```