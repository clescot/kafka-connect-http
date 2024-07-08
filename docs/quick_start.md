# Quick start guide

Minimal configuration to interact with HTTP servers.

## Configuration ignoring HTTP results

### Sink Connector configuration

This configuration does NOT publish HTTP results (neither with a low level producer, nor to an in memory queue).

```json 
{
    "name": "my-http-sink-connector",
    "config": {
    "connector.class":"io.github.clescot.kafka.connect.http.sink.HttpSinkConnector",
    "tasks.max": "1",
    "topics":"http-request"
    }
}
```

## Configuration to call web sites and publish results

Multiple configurations are available : 

### Sink connector + direct low level producer with string output

```json 
{
    "name": "my-http-sink-connector",
    "config": {
    "connector.class":"io.github.clescot.kafka.connect.http.sink.HttpSinkConnector",
    "tasks.max": "1",
    "topics":"http-request",
    "publish.mode":"PRODUCER",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter.use.optional.for.nonrequired": "true",
    "producer.format": "string",
    "producer.bootstrap.servers": "kafka:9092",
    "producer.schema.registry.url": "https://myschemaregistry.com:8081",
    "producer.success.topic": "http-success",
    "producer.error.topic": "http-error"
    }
}
```
### Sink connector + direct low level producer with json output

```json 
{
    "name": "my-http-sink-connector",
    "config": {
    "connector.class":"io.github.clescot.kafka.connect.http.sink.HttpSinkConnector",
    "tasks.max": "1",
    "topics":"http-request",
    "publish.mode":"PRODUCER",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter.use.optional.for.nonrequired": "true",
    "producer.format": "json",
    "producer.bootstrap.servers": "kafka:9092",
    "producer.schema.registry.url": "https://myschemaregistry.com:8081",
    "producer.success.topic": "http-success",
    "producer.error.topic": "http-error"
    }
}
```


### Sink connector + In memory Queue + Source connector
#### Sink connector configuration

This configuration publish HTTP results to an in memory queue.

```json 
{
    "name": "my-http-sink-connector",
    "config": {
    "connector.class":"io.github.clescot.kafka.connect.http.sink.HttpSinkConnector",
    "tasks.max": "1",
    "topics":"http-request",
    "publish.mode":"IN_MEMORY_QUEUE"
    }
}
```

>Warning: when the option `publish.to.in.memory.queue` is set to `true`, a source connector on the same kafka connect instance is mandatory. 

#### Source Connector configuration

This configuration listen to the in memory queue, to publish HTTP results in the configured `success.topic` or `error.topic`topic,
depending on the HTTP result.

```json 
{
    "name": "my-http-source-connector",
    "config": {
    "connector.class":"io.github.clescot.kafka.connect.http.source.HttpSourceConnector",
    "tasks.max": "1",
    "success.topic": "http-success",
    "error.topic": "http-error"
    }
}
```