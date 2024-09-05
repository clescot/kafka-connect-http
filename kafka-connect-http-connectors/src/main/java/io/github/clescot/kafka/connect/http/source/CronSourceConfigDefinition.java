package io.github.clescot.kafka.connect.http.source;

import org.apache.kafka.common.config.ConfigDef;


public class CronSourceConfigDefinition {

    public static final String TOPIC = "topic";
    public static final String TOPIC_DOC = "Topic to receive http request to execute";
    public static final String JOBS = "jobs";
    public static final String JOBS_DOC = "Topic to receive http request to execute";
    private CronSourceConfigDefinition() {
        //Class with only static methods
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(TOPIC, ConfigDef.Type.STRING,ConfigDef.Importance.HIGH, TOPIC_DOC)
                .define(JOBS,ConfigDef.Type.LIST,ConfigDef.Importance.HIGH, JOBS_DOC);
    }
}
