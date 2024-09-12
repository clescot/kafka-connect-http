package io.github.clescot.kafka.connect.http.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class CronSourceConnectorConfig extends AbstractConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(CronSourceConnectorConfig.class);
    public static final String CANNOT_BE_FOUND_IN_MAP_CONFIGURATION = " cannot be found in map configuration";
    private final String topic;
    private List<String> jobIds;


    public CronSourceConnectorConfig(Map<?, ?> originals) {
        this(CronSourceConfigDefinition.config(), originals);
    }

    public CronSourceConnectorConfig(ConfigDef configDef, Map<?, ?> originals){
        super(configDef,originals);
        this.topic = Optional.ofNullable(getString(CronSourceConfigDefinition.TOPIC)).orElseThrow(()-> new IllegalArgumentException(CronSourceConfigDefinition.TOPIC + CANNOT_BE_FOUND_IN_MAP_CONFIGURATION));
        this.jobIds  = getList(CronSourceConfigDefinition.JOBS);
    }

    public String getTopic() {
        return topic;
    }

    public List<String> getJobs() {
        return jobIds;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CronSourceConnectorConfig)) return false;
        if (!super.equals(o)) return false;
        CronSourceConnectorConfig that = (CronSourceConnectorConfig) o;
        return Objects.equals(topic, that.topic) && Objects.equals(jobIds, that.jobIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), topic, jobIds);
    }
}
