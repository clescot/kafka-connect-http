package io.github.clescot.kafka.connect.sse.client.okhttp;

import com.google.common.base.Preconditions;
import io.github.clescot.kafka.connect.http.VersionUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SseSourceConnector extends SourceConnector {

    private static final VersionUtils VERSION_UTILS = new VersionUtils();
    private SseConnectorConfig sseConnectorConfig;
    private Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        this.sseConnectorConfig = new SseConnectorConfig(config(),props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SseSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Preconditions.checkArgument(maxTasks>0,"maxTasks must be higher than 0");
        Preconditions.checkNotNull(this.sseConnectorConfig, "sseConnectorConfig must not be null. Call start() first.");
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(this.sseConnectorConfig.originalsStrings());
        }

        return configs;
    }

    @Override
    public void stop() {
        // No specific stop logic needed for this connector.
    }

    @Override
    public ConfigDef config() {
        Preconditions.checkNotNull(props, "props must not be null. Call start() first.");
        SseConfigDefinition sseConfigDefinition = new SseConfigDefinition(props);
        return sseConfigDefinition.config();
    }

    @Override
    public String version() {
        return VERSION_UTILS.getVersion();
    }
}
