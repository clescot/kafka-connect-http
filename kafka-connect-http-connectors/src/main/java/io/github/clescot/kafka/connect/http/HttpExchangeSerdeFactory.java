package io.github.clescot.kafka.connect.http;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class HttpExchangeSerdeFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpExchangeSerdeFactory.class);
    private final SchemaRegistryClient schemaRegistryClient;
    private final Map<String, Object> serdeConfig;


    public HttpExchangeSerdeFactory(SchemaRegistryClient schemaRegistryClient,
                                    Map<String, Object> serdeConfig) {
        this.schemaRegistryClient = schemaRegistryClient;
        this.serdeConfig = serdeConfig;
    }

    public Serde<HttpExchange> buildValueSerde(){
        final KafkaJsonSchemaSerde<HttpExchange> jsonSchemaSerde = new KafkaJsonSchemaSerde<>(schemaRegistryClient,HttpExchange.class);
        serdeConfig.entrySet().forEach(entry-> LOGGER.info("{}:{}",entry.getKey(),entry.getValue()));
        jsonSchemaSerde.configure(serdeConfig, false);
        return jsonSchemaSerde;
    }
}
