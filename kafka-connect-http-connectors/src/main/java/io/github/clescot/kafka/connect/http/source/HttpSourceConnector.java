package io.github.clescot.kafka.connect.http.source;

import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HttpSourceConnector extends SourceConnector {

    private HttpSourceConnectorConfig httpSourceConnectorConfig;
    @Override
    public void start(Map<String, String> props) {
        this.httpSourceConnectorConfig = new HttpSourceConnectorConfig(config(),props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HttpSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(this.httpSourceConnectorConfig.originalsStrings());
        }

        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return HttpSourceConfigDefinition.config();
    }

    @Override
    public String version() {
        return QueueFactory.VersionUtil.version(this.getClass());
    }
}
