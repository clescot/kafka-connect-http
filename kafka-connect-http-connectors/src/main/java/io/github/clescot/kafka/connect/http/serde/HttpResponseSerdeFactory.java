package io.github.clescot.kafka.connect.http.serde;

import com.google.common.base.Preconditions;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class HttpResponseSerdeFactory implements SerdeFactory<HttpResponse> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpResponseSerdeFactory.class);
    private final SchemaRegistryClient schemaRegistryClient;
    private final Map<String, Object> serdeConfig;


    public HttpResponseSerdeFactory(SchemaRegistryClient schemaRegistryClient,
                                    Map<String, Object> serdeConfig) {
        Preconditions.checkNotNull(schemaRegistryClient);
        Preconditions.checkNotNull(serdeConfig);
        this.schemaRegistryClient = schemaRegistryClient;
        this.serdeConfig = serdeConfig;
    }

    @Override
    public Serde<HttpResponse> buildSerde(boolean recordKey){
        final KafkaJsonSchemaSerde<HttpResponse> jsonSchemaSerde = new KafkaJsonSchemaSerde<>(schemaRegistryClient,HttpResponse.class);
        serdeConfig.forEach((key, value) -> LOGGER.info("{}:{}", key, value));
        jsonSchemaSerde.configure(serdeConfig, recordKey);
        return jsonSchemaSerde;
    }
}
