package com.github.clescot.kafka.connect.http.sink;

import com.google.common.base.Preconditions;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class HttpSinkConnector extends SinkConnector {

    private HttpSinkConnectorConfig wsSinkConnectorConfig;

    @Override
    public void start(Map<String, String> settings) {
        Preconditions.checkNotNull(settings);
        this.wsSinkConnectorConfig = new HttpSinkConnectorConfig(config(),settings);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HttpSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int taskCount) {
        List<Map<String, String>> configs = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            configs.add(this.wsSinkConnectorConfig.originalsStrings());
        }
        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return HttpSinkConfigDefinition.config();
    }

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }
}
