# HTTP Connectors configuration

Here are the configuration to setup an HTTP Sink connector, and optionally an HTTP Source Connector, into a Kafka Connect
cluster. Note that the jar file owning these connector classes,
[need to be installed with the Kafka connect runtime](https://docs.confluent.io/kafka-connectors/self-managed/install.html#install-connector-manually).

### HTTP Sink Connector

#### required parameters

every Kafka Connect Sink Connector need to define these required parameters :

- *connector.class* : `com.github.clescot.kafka.connect.http.sink.HttpSinkConnector`
- *topics* (or *topics.regex*): `http-requests` for example

#### optional Kafka Connect parameters

- *tasks.max*  (default to `1`)
- *key.converter*
- *value.converter*
- ....

#### optional HTTP Sink connector parameters

- *publish.to.in.memory.queue* : `false` by default. When set to `true`, publish HTTP interactions (request and responses)
  are published into the in memory queue.
- *queue.name* : if not set, `default` queue name is used, if the `publish.to.in.memory.queue` is set to `true`.
  You can define multiple in memory queues, to permit to publish to different topics, different HTTP interactions. If
  you set this parameter to a value different than `default`, you need to configure an HTTP source Connector listening
  on the same queue name to avoid some OutOfMemoryErrors.
- *static.request.header.names* : list of headers names to attach to all requests. *Static* term, means that these headers
  are not managed by initial kafka message, but are defined at the connector level and added globally. this list is divided by
  `,` character. The connector will try to get the value to add to request by querying the config with the header name as parameter name.
  For example, if set `static.request.header.names: param_name1,param_name2`, the connector will lookup the param_name1
  and param_name2 parameters to get values to add.
- default retry policy parameters : these parameters (**set them all or no one**), permit to define a default retry policy, which can be overriden by parameters set at the request level.
  - *default.retries* : how many retries
  - *default.retry.delay.in.ms* : initial delay between retries
  - *default.retry.max.delay.in.ms* :max delay between retries
  - *default.retry.delay.factor* : by which number multiply the previous delay to calculate the current one
  - *default.retry.jitter.in.ms* : add a random factor to avoid multiple retry policies firing at the same time.
- *generate.missing.request.id* : `false` by default. when set to `true`, generate an uuid bound to the 'X-Request-ID' header.
- *generate.missing.correlation.id* : `false` by default. when set to `true`, generate an uuid bound to the 'X-Correlation-ID' header.

#### Configuration example

- `sink.json` example :
```json 
{
    "name": "my-http-sink-connector",
    "config": {
    "connector.class":"com.github.clescot.kafka.connect.http.sink.HttpSinkConnector",
    "tasks.max": "1",
    "topics":"http-request",
    }
}
```
- `sink.json` example _with publishment in the in memory queue_, for the Source Connector:
```json 
{
    "name": "my-http-sink-connector",
    "config": {
    "connector.class":"com.github.clescot.kafka.connect.http.sink.HttpSinkConnector",
    "tasks.max": "1",
    "topics":"http-request",
    "publish.to.in.memory.queue":"true"
    }
}
```

- a more advanced example :

```json 
{
  "name": "my-http-sink-connector",
  "config": {
    "connector.class":"com.github.clescot.kafka.connect.http.sink.HttpSinkConnector",
    "tasks.max": "1",
    "topics":"http-request",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.json.JsonSchemaConverter",
    "value.converter.use.optional.for.nonrequired": "true",
    "value.converter.schema.registry.url": "https://my-schema-registry-url:8081",
    "generate.missing.request.id": "true",
    "generate.missing.correlation.id": "true",
    "publish.to.in.memory.queue":"true",
    "default.retries":"3",
    "default.retry.delay.in.ms":"2000",
    "default.retry.max.delay.in.ms":"600000",
    "default.retry.delay.factor":"1.5",
    "default.retry.jitter.in.ms":"500",
    "default.rate.limiter.period.in.ms":"1000",
    "default.rate.limiter.max.executions":"5",
    "httpclient.ssl.truststore.path": "/path/to/my.truststore.jks",
    "httpclient.ssl.truststore.password": "mySecret_Pass",
    "httpclient.ssl.truststore.type": "jks"
  }
}
```

You can create or update this connector instance with this command :

```bash
curl -X PUT -H "Content-Type: application/json" --data @sink.json http://my-kafka-connect-cluster:8083/connectors/my-http-sink-connector/config
```
### HTTP Source Connector

The HTTP Source connector is only useful if you have configured the publishment of HTTP interactions into the queue,
via the `publish.to.in.memory.queue` set to `true`.

#### required HTTP Source connector parameters

- *success.topic* : Topic to receive successful http request/responses, for example, http-success
- *error.topic* : Topic to receive errors from http request/responses, for example, http-error

#### optional HTTP Source connector parameters

- *queue.name* : if not set, listen on the 'default' queue.

#### Configuration example


`source.json` example :
```json 
{
    "name": "my-http-source-connector",
    "config": {
    "connector.class":"com.github.clescot.kafka.connect.http.source.HttpSourceConnector",
    "tasks.max": "1",
    "success.topic": "http-success",
    "error.topic": "http-error",
    }
}
```

You can create or update this connector instance with this command :

```bash
curl -X PUT -H "Content-Type: application/json" --data @source.json http://my-kafka-connect-cluster:8083/connectors/my-http-source-connector/config
```

### Input and output Topics partitionning

# TODO 
