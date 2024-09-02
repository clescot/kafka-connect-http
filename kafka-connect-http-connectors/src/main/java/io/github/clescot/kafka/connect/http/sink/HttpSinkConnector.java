package io.github.clescot.kafka.connect.http.sink;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.VersionUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;
import static io.github.clescot.kafka.connect.http.sink.HttpSinkTask.DEFAULT_CONFIGURATION_ID;


public class HttpSinkConnector extends SinkConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSinkConnector.class);
    private HttpSinkConnectorConfig httpSinkConnectorConfig;
    private static final VersionUtils VERSION_UTILS = new VersionUtils();
    private Map<String, String> settings;

    @Override
    public void start(Map<String, String> settings) {
        this.settings = settings;
        Preconditions.checkNotNull(settings);
        this.httpSinkConnectorConfig = new HttpSinkConnectorConfig(config(),settings);
    }

    @Override
    public Class<? extends Task> taskClass() {
        Map<String, Object> defaultConfigurationSettings = httpSinkConnectorConfig.originalsWithPrefix("config." + DEFAULT_CONFIGURATION_ID + ".");
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
        List<Map<String, String>> configs = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            configs.add(this.httpSinkConnectorConfig.originalsStrings());
        }
        return configs;
    }

    @Override
    public void stop() {
        //no external dependencies to clear.
    }

    @Override
    public ConfigDef config() {
        HttpSinkConfigDefinition httpSinkConfigDefinition = new HttpSinkConfigDefinition(Optional.ofNullable(settings).orElse(Maps.newHashMap()));
        return httpSinkConfigDefinition.config();
    }

    @Override
    public String version() {
        return VERSION_UTILS.getVersion();
    }
}
