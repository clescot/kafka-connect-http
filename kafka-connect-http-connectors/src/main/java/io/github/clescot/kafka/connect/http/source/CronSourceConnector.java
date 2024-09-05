package io.github.clescot.kafka.connect.http.source;

import io.github.clescot.kafka.connect.http.VersionUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CronSourceConnector extends SourceConnector {

    private static final VersionUtils VERSION_UTILS = new VersionUtils();
    private CronSourceConnectorConfig cronSourceConnectorConfig;
    @Override
    public void start(Map<String, String> props) {
        this.cronSourceConnectorConfig = new CronSourceConnectorConfig(config(),props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return null;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(this.cronSourceConnectorConfig.originalsStrings());
        }

        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return CronSourceConfigDefinition.config();
    }

    @Override
    public String version() {
        return VERSION_UTILS.getVersion();
    }
}
