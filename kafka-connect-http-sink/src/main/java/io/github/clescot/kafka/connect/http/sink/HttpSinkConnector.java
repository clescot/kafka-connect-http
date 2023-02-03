package io.github.clescot.kafka.connect.http.sink;

import io.github.clescot.kafka.connect.http.core.core.queue.QueueFactory;
import com.google.common.base.Preconditions;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class HttpSinkConnector extends SinkConnector {

    private HttpSinkConnectorConfig httpSinkConnectorConfig;

    @Override
    public void start(Map<String, String> settings) {
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
        return HttpSinkConfigDefinition.config();
    }

    @Override
    public String version() {
        return QueueFactory.VersionUtil.version(this.getClass());
    }
}
