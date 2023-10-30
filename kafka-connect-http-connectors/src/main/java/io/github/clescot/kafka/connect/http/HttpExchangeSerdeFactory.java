package io.github.clescot.kafka.connect.http;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import org.apache.kafka.common.serialization.Serde;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import java.util.Map;

public class HttpExchangeSerdeFactory {


    private final SchemaRegistryClient schemaRegistryClient;

    private final JsonSchemaSerdeConfigFactory jsonSchemaSerdeFactory;

    public HttpExchangeSerdeFactory(SchemaRegistryClient schemaRegistryClient,
                                    JsonSchemaSerdeConfigFactory jsonSchemaSerdeFactory) {
        this.schemaRegistryClient = schemaRegistryClient;
        this.jsonSchemaSerdeFactory = jsonSchemaSerdeFactory;
    }

    public Serde<HttpExchange> buildValueSerde(){
        final KafkaJsonSchemaSerde<HttpExchange> jsonSchemaSerde = new KafkaJsonSchemaSerde<>(schemaRegistryClient,HttpExchange.class);
        Map<String, Object> serdeConfig = jsonSchemaSerdeFactory.getConfig();
        jsonSchemaSerde.configure(serdeConfig, false);
        return jsonSchemaSerde;
    }
}
