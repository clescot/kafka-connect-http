package io.github.clescot.kafka.connect.http.source.cron;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class HttpCronSourceConnectorConfig extends AbstractConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpCronSourceConnectorConfig.class);
    public static final String CANNOT_BE_FOUND_IN_MAP_CONFIGURATION = " cannot be found in map configuration";
    private final String topic;
    private List<String> jobIds;


    public HttpCronSourceConnectorConfig(Map<?, ?> originals) {
        this(HttpCronSourceConfigDefinition.config(), originals);
    }

    public HttpCronSourceConnectorConfig(ConfigDef configDef, Map<?, ?> originals){
        super(configDef,originals);
        this.topic = Optional.ofNullable(getString(HttpCronSourceConfigDefinition.TOPIC)).orElseThrow(()-> new IllegalArgumentException(HttpCronSourceConfigDefinition.TOPIC + CANNOT_BE_FOUND_IN_MAP_CONFIGURATION));
        this.jobIds  = getList(HttpCronSourceConfigDefinition.JOBS);
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
        if (!(o instanceof HttpCronSourceConnectorConfig)) return false;
        if (!super.equals(o)) return false;
        HttpCronSourceConnectorConfig that = (HttpCronSourceConnectorConfig) o;
        return Objects.equals(topic, that.topic) && Objects.equals(jobIds, that.jobIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), topic, jobIds);
    }
}
