## HTTP exchange outcome format

When the Sink HTTP connector is configured to publish messages (`HTTPExchange`) in the in memory queue (via the `publish.to.in.memory.queue` 
option set to `true`), The HTTP Source Connector can be configured, to receive these messages and send them to a success or 
error topic.
These messages (`HTTPExchange`), are `Struct` messages.

### Struct format

```java
    public static final Schema SCHEMA = SchemaBuilder
            .struct()
            .name(HttpResponse.class.getName())
            .version(VERSION)
            .field(STATUS_CODE,Schema.INT64_SCHEMA)
            .field(STATUS_MESSAGE,Schema.STRING_SCHEMA)
            .field(HEADERS, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA)).build())
            .field(BODY,Schema.STRING_SCHEMA);

```

### JSON Schema format

[JSON Schema Format](src/test/resources/http-exchange.json)
