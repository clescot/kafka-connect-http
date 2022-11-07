package com.github.clescot.kafka.connect.http.source;

import com.github.clescot.kafka.connect.http.sink.config.ConfigDefinition;
import com.github.clescot.kafka.connect.http.sink.utils.VersionUtil;
import com.google.common.base.Preconditions;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WsSourceConnector extends SourceConnector {


    private Map<String, String> settings;

    @Override
    public void start(Map<String, String> props) {
        Preconditions.checkNotNull(settings);
        this.settings = settings;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return WsSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(this.settings);
        }
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return ConfigDefinition.config();
    }

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }
}
