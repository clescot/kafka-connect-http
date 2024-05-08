## HTTP exchange outcome format

When the Sink HTTP connector is configured to publish messages (`HTTPExchange`) in the in memory queue (via the `publish.to.in.memory.queue` 
option set to `true`), The HTTP Source Connector can be configured, to receive these messages and send them to a success or 
error topic.
These messages (`HTTPExchange`), are `Struct` messages.

### Struct format

```java
public class HTTPExchange {
    public final static Schema SCHEMA = SchemaBuilder
        .struct()
        .name(HttpExchange.class.getName())
        .version(HTTP_EXCHANGE_VERSION)
        //metadata fields
        .field(DURATION_IN_MILLIS, Schema.INT64_SCHEMA)
        .field(MOMENT, Schema.STRING_SCHEMA)
        .field(ATTEMPTS, Schema.INT32_SCHEMA)
        //request
        .field(REQUEST, HttpRequest.SCHEMA)
        // response
        .field(RESPONSE, HttpResponse.SCHEMA)
        .schema();
}
```

### JSON Schema format

[JSON Schema Format](../kafka-connect-http-core/src/main/resources/schemas/json/versions/1/http-exchange.json)
