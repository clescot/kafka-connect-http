package io.github.clescot.kafka.connect.http.source.cron;

import com.google.common.base.Preconditions;
import io.github.clescot.kafka.connect.http.VersionUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * CronSourceConnector is a Kafka Connect Source Connector that triggers HTTP requests based on a cron schedule.
 * It uses Quartz Scheduler to manage the scheduling of HTTP requests.
 */
public class CronSourceConnector extends SourceConnector {

    private static final VersionUtils VERSION_UTILS = new VersionUtils();
    private HttpCronSourceConnectorConfig httpCronSourceConnectorConfig;
    @Override
    public void start(Map<String, String> props) {
        this.httpCronSourceConnectorConfig = new HttpCronSourceConnectorConfig(config(),props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HttpCronSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Preconditions.checkArgument(maxTasks>0,"maxTasks must be higher than 0");
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(this.httpCronSourceConnectorConfig.originalsStrings());
        }

        return configs;
    }

    @Override
    public void stop() {
        //nothing to do
    }

    @Override
    public ConfigDef config() {
        return HttpCronSourceConfigDefinition.config();
    }

    @Override
    public String version() {
        return VERSION_UTILS.getVersion();
    }
}
