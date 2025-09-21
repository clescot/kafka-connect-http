package io.github.clescot.kafka.connect.http.source.queue;

import io.github.clescot.kafka.connect.http.core.VersionUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.github.clescot.kafka.connect.http.core.VersionUtils.VERSION;

public class HttpInMemoryQueueSourceConnector extends SourceConnector {
    private HttpSourceConnectorConfig httpSourceConnectorConfig;
    @Override
    public void start(Map<String, String> props) {
        this.httpSourceConnectorConfig = new HttpSourceConnectorConfig(config(),props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HttpInMemoryQueueSourceTask.class;
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
        //nothing to stop
    }

    @Override
    public ConfigDef config() {
        return HttpInMemoryQueueSourceConfigDefinition.config();
    }

    @Override
    public String version() {
        return VERSION;
    }
}
