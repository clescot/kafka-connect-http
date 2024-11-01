package io.github.clescot.kafka.connect.http.serde;

import com.google.common.base.Preconditions;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class HttpExchangeSerdeFactory implements SerdeFactory<HttpExchange> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpExchangeSerdeFactory.class);
    private final SchemaRegistryClient schemaRegistryClient;
    private final Map<String, Object> serdeConfig;


    public HttpExchangeSerdeFactory(SchemaRegistryClient schemaRegistryClient,
                                    Map<String, Object> serdeConfig) {
        Preconditions.checkNotNull(schemaRegistryClient);
        Preconditions.checkNotNull(serdeConfig);
        this.schemaRegistryClient = schemaRegistryClient;
        this.serdeConfig = serdeConfig;
    }

    @Override
    public Serde<HttpExchange> buildSerde(boolean recordKey){
        final KafkaJsonSchemaSerde<HttpExchange> jsonSchemaSerde = new KafkaJsonSchemaSerde<>(schemaRegistryClient,HttpExchange.class);
        serdeConfig.forEach((key, value) -> LOGGER.info("{}:{}", key, value));
        jsonSchemaSerde.configure(serdeConfig, recordKey);
        return jsonSchemaSerde;
    }
}
