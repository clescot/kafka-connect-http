package io.github.clescot.kafka.connect.http.source.cron;

import com.google.common.base.Preconditions;
import io.github.clescot.kafka.connect.MapUtils;
import io.github.clescot.kafka.connect.http.core.VersionUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.github.clescot.kafka.connect.http.core.VersionUtils.VERSION;

/**
 * CronSourceConnector is a Kafka Connect Source Connector that triggers HTTP requests based on a cron schedule.
 * It uses Quartz Scheduler to manage the scheduling of HTTP requests.
 */
public class CronSourceConnector extends SourceConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(CronSourceConnector.class);
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
        Preconditions.checkNotNull(httpCronSourceConnectorConfig, "httpCronSourceConnectorConfig must not be null. Call start() first.");
        int numGroups = Math.min(httpCronSourceConnectorConfig.getJobs().size(), maxTasks);
        List<List<String>> partitions = ConnectorUtils.groupPartitions(httpCronSourceConnectorConfig.getJobs(), numGroups);
        for (List<String> partition : partitions) {
            List<String> list = partition.stream().map(jobId -> "job." + jobId).toList();
            Map<String, String> subSettings = MapUtils.filterEntriesStartingWithPrefixes(httpCronSourceConnectorConfig.originalsStrings(), list.toArray(new String[0]));
            subSettings.put("jobs", String.join(",", partition));
            subSettings.putAll(
                    // Filter out job-specific settings and keep only the common settings
                    httpCronSourceConnectorConfig
                            .originalsStrings()
                            .entrySet()
                            .stream()
                            .filter(entry-> !entry.getKey().startsWith("jobs"))
                            .filter(entry-> !entry.getKey().startsWith("job."))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
            configs.add(subSettings);
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
        return VERSION;
    }
}
