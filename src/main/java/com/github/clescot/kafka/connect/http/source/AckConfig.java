package com.github.clescot.kafka.connect.http.source;

import com.github.clescot.kafka.connect.http.sink.config.ConfigConstants;
import com.github.clescot.kafka.connect.http.sink.config.ConfigDefinition;
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
    private final String ackTopic;


    public AckConfig(Map<?, ?> originals) {
        super(ConfigDefinition.config(), originals);
        Preconditions.checkNotNull(originals,"map configuration for AckConfig cannot be null");
        this.ackTopic = Optional.ofNullable(getString(ConfigConstants.ACK_TOPIC)).orElseThrow(()-> new IllegalArgumentException(ConfigConstants.ACK_TOPIC+ CANNOT_BE_FOUND_IN_MAP_CONFIGURATION));
    }




    public String getAckTopic() {
        return ackTopic;
    }


}
