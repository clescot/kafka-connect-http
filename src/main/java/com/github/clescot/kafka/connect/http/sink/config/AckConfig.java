package com.github.clescot.kafka.connect.http.sink.config;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;

import java.util.Map;
import java.util.Optional;

public class AckConfig extends AbstractConfig {

    public static final String CANNOT_BE_FOUND_IN_MAP_CONFIGURATION = " cannot be found in map configuration";
    private final String targetBootstrapServer;
    private final String targetSchemaRegistry;
    private final String ackTopic;
    private final String producerClientId;
    private final Schema ackSchema;


    public AckConfig(Map<?, ?> originals) {
        super(ConfigDefinition.config(), originals);
        Preconditions.checkNotNull(originals,"map configuration for AckConfig cannot be null");
        this.targetBootstrapServer = Optional.ofNullable(getString(ConfigConstants.TARGET_BOOTSTRAP_SERVER)).orElseThrow(()-> new IllegalArgumentException(ConfigConstants.TARGET_BOOTSTRAP_SERVER+ CANNOT_BE_FOUND_IN_MAP_CONFIGURATION));
        this.targetSchemaRegistry = Optional.ofNullable(getString(ConfigConstants.TARGET_SCHEMA_REGISTRY)).orElseThrow(()-> new IllegalArgumentException(ConfigConstants.TARGET_SCHEMA_REGISTRY+ CANNOT_BE_FOUND_IN_MAP_CONFIGURATION));
        this.producerClientId = Optional.ofNullable(getString(ConfigConstants.PRODUCER_CLIENT_ID)).orElseThrow(()-> new IllegalArgumentException(ConfigConstants.PRODUCER_CLIENT_ID+ CANNOT_BE_FOUND_IN_MAP_CONFIGURATION));
        this.ackTopic = Optional.ofNullable(getString(ConfigConstants.ACK_TOPIC)).orElseThrow(()-> new IllegalArgumentException(ConfigConstants.ACK_TOPIC+ CANNOT_BE_FOUND_IN_MAP_CONFIGURATION));

        this.ackSchema = getSchemaFromAvroConfig();
    }

    private Schema getSchemaFromAvroConfig() {
        AvroData avroData = new AvroData(new AvroDataConfig(ImmutableMap.of(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, targetSchemaRegistry)));
        String ackSchemaParam = Optional.ofNullable(getString(ConfigConstants.ACK_SCHEMA)).orElseThrow(()-> new IllegalArgumentException(ConfigConstants.ACK_SCHEMA+ CANNOT_BE_FOUND_IN_MAP_CONFIGURATION));
        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(ackSchemaParam);
         return avroData.toConnectSchema(avroSchema);
    }

    public String getTargetBootstrapServer() {
        return targetBootstrapServer;
    }

    public String getTargetSchemaRegistry() {
        return targetSchemaRegistry;
    }

    public String getAckTopic() {
        return ackTopic;
    }

    public String getProducerClientId() {
        return producerClientId;
    }

    public Schema getAckSchema() {
        return ackSchema;
    }
}
