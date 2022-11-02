package com.github.clescot.kafka.connect.http.source;

import com.github.clescot.kafka.connect.http.sink.config.ConfigConstants;
import com.github.clescot.kafka.connect.http.sink.config.ConfigDefinition;
import org.apache.kafka.common.config.AbstractConfig;

import java.util.Map;
import java.util.Optional;

public class AckConfig extends AbstractConfig {

    public static final String CANNOT_BE_FOUND_IN_MAP_CONFIGURATION = " cannot be found in map configuration";
    private final String ackTopic;


    public AckConfig(Map<?, ?> originals) {
        super(ConfigDefinition.config(), originals);
        this.ackTopic = Optional.ofNullable(getString(ConfigConstants.SUCCESS_TOPIC)).orElseThrow(()-> new IllegalArgumentException(ConfigConstants.SUCCESS_TOPIC + CANNOT_BE_FOUND_IN_MAP_CONFIGURATION));
    }




    public String getAckTopic() {
        return ackTopic;
    }


}
