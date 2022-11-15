# Incoming Message Format : HTTP Request

## Kafka Connect Architecture

### low level serialization
Kafka, at the low level, handle transformation of an Object in a language (Java, Python, etc...) from the Kafka client to bytes via _serializers_, which are transmitted on the network to the Kafka Broker.

But when the message is transmitted, both ends are linked to the message format.

### high-level serialization and format independance with Kafka Connect

Kafka Connect try to solve this coupling problem, with `Converters`, which internally owns a configured low level serializer.

Converters are configured per connector instance.

Converters transform a `byte array`, into a java `Object`, linked to a Schema,describing the object structure (`org.apache.kafka.connect.data.SchemaAndValue`).
Most of converters, like `JSONConverter`, `JSONSchemaConverter`, `AvroConverter`, and `ProtobufConverter`, but not `StringConverter`,
convert internally their native format (respecctively `JSON`, `JSONSchema`, `Avro`, and `Protobuf`),
to a `Struct` instance, representing a structured message, via some internal helpers (`JsonSchemaData`,`AvroData`,`ProtobufData`).

### What impact of this Kafka Connect design for the Http Sink Connector ?

The HTTP Sink Connector, support `structured messages`, coming from different formats (JSON Schema, Avro, and Protobuf),
but tests have been done only on JSON Schema. This is the main use case.
Due to some format-related tricks which cannot be hidden by the kafka conncet strategy (for example, supported types and so on..),
Avro and Protobuf should work as incoming format, but without guarantee.

The HTTP Sink Connector, support also String messages, but not as the main use case.

## HTTP Request incoming Format

### fields

#### connection fields
- timeoutInMs

##### retry policy fields


- retries
- retryDelayInMs
- retryMaxDelayInMs
- retryDelayFactor
- retryJitter

#### Request Fields

- url
- method
- headers
- bodyType
- bodyAsString
- bodyAsByteArray
- bodyAsMultipart

### Struct format

The `HttpRequest` class represents the incoming message.

Here is the source code which define the Struct format, base source for JSON Schema/Avro/Protobuf solutions : 

```java
    public static final Schema SCHEMA = SchemaBuilder
        .struct()
        .name(HttpRequest.class.getName())
        .version(VERSION)
        //meta-data outside of the request
        //connection (override the default one set in the Sink Connector)
        .field(TIMEOUT_IN_MS, Schema.OPTIONAL_INT64_SCHEMA)
        //retry policy (override the default one set in the Sink Connector)
        .field(RETRIES, Schema.OPTIONAL_INT32_SCHEMA)
        .field(RETRY_DELAY_IN_MS, Schema.OPTIONAL_INT64_SCHEMA)
        .field(RETRY_MAX_DELAY_IN_MS, Schema.OPTIONAL_INT64_SCHEMA)
        .field(RETRY_DELAY_FACTOR, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(RETRY_JITTER, Schema.OPTIONAL_INT64_SCHEMA)
        //request
        .field(HEADERS, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA)).build())
        .field(URL, Schema.STRING_SCHEMA)
        .field(METHOD, Schema.STRING_SCHEMA)
        .field(BODY_TYPE, Schema.STRING_SCHEMA)
        .field(BODY_AS_STRING, Schema.OPTIONAL_STRING_SCHEMA)
        .field(BODY_AS_BYTE_ARRAY, Schema.OPTIONAL_STRING_SCHEMA)
        .field(BODY_AS_MULTIPART, SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA));
```

### JSON Schema format

[HTTP Request JSON Schema Format](src/test/resources/http-request.json)

