package io.github.clescot.kafka.connect.http.sink;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.VersionUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.github.clescot.kafka.connect.Configuration.DEFAULT_CONFIGURATION_ID;
import static io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition.*;


public class HttpSinkConnector extends SinkConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSinkConnector.class);
    private HttpConnectorConfig httpConnectorConfig;
    private static final VersionUtils VERSION_UTILS = new VersionUtils();
    private Map<String, String> settings;

    @Override
    public void start(Map<String, String> settings) {
        this.settings = settings;
        Preconditions.checkNotNull(settings);
        this.httpConnectorConfig = new HttpConnectorConfig(config(),settings);
    }

    @Override
    public Class<? extends Task> taskClass() {
        Map<String, Object> defaultConfigurationSettings = httpConnectorConfig.originalsWithPrefix("config." + DEFAULT_CONFIGURATION_ID + ".");
        String httpClientImplementation = (String) Optional.ofNullable(defaultConfigurationSettings.get(CONFIG_HTTP_CLIENT_IMPLEMENTATION)).orElse(OKHTTP_IMPLEMENTATION);
        if (AHC_IMPLEMENTATION.equalsIgnoreCase(httpClientImplementation)) {
           return AHCSinkTask.class;
        } else if (OKHTTP_IMPLEMENTATION.equalsIgnoreCase(httpClientImplementation)) {
           return OkHttpSinkTask.class;
        } else {
            LOGGER.error("unknown HttpClient implementation : must be either 'ahc' or 'okhttp', but is '{}'", httpClientImplementation);
            throw new IllegalArgumentException("unknown HttpClient implementation : must be either 'ahc' or 'okhttp', but is '" + httpClientImplementation + "'");
        }

    }

    @Override
    public List<Map<String, String>> taskConfigs(int taskCount) {
        Preconditions.checkNotNull(settings,"settings must not be null. Call start() first.");
        Preconditions.checkArgument(!settings.isEmpty(),"settings must not be empty.");
        Preconditions.checkArgument(!this.httpConnectorConfig.getConfigurationIds().isEmpty());
        Preconditions.checkArgument(this.httpConnectorConfig.getConfigurationIds().contains(DEFAULT_CONFIGURATION_ID));
        List<Map<String, String>> configs = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            //all tasks have all the configurations, to handle all Http requests.
            //scalability with multiple connector instances is handled by partitioning the incoming topic.
            //each task will have one or more partitions to handle.
            configs.add(this.httpConnectorConfig.originalsStrings());
        }
        return configs;
    }

    @Override
    public void stop() {
        //no external dependencies to clear.
    }

    @Override
    public ConfigDef config() {
        HttpConfigDefinition httpConfigDefinition = new HttpConfigDefinition(Optional.ofNullable(settings).orElse(Maps.newHashMap()));
        return httpConfigDefinition.config();
    }

    @Override
    public String version() {
        return VERSION_UTILS.getVersion();
    }
}
