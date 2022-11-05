package com.github.clescot.kafka.connect.http.source;

import com.github.clescot.kafka.connect.http.sink.VersionUtil;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WsSourceConnector extends SourceConnector {

    private WsSourceConnectorConfig wsSourceConnectorConfig;
    @Override
    public void start(Map<String, String> props) {
        this.wsSourceConnectorConfig = new WsSourceConnectorConfig(config(),props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return WsSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(this.wsSourceConnectorConfig.originalsStrings());
        }

        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return SourceConfigDefinition.config();
    }

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }
}
