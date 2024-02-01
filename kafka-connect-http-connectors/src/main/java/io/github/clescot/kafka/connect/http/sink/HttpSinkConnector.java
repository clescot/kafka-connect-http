package io.github.clescot.kafka.connect.http.sink;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.VersionUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;


public class HttpSinkConnector extends SinkConnector {

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
        return HttpSinkTask.class;
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
