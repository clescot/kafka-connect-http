package io.github.clescot.kafka.connect.sse.client.okhttp;

import io.github.clescot.kafka.connect.http.VersionUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.List;
import java.util.Map;

public class SseSourceConnector extends SourceConnector {

    private static final VersionUtils VERSION_UTILS = new VersionUtils();

    @Override
    public void start(Map<String, String> map) {

    }

    @Override
    public Class<? extends Task> taskClass() {
        return SseSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        return List.of();
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public String version() {
        return "";
    }
}
