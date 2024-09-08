package io.github.clescot.kafka.connect.http.source;

import com.google.common.base.Preconditions;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class CronSourceConnectorConfig extends AbstractConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(CronSourceConnectorConfig.class);
    public static final String CANNOT_BE_FOUND_IN_MAP_CONFIGURATION = " cannot be found in map configuration";
    private final String topic;
    private List<String> jobIds;


    public CronSourceConnectorConfig(Map<?, ?> originals) {
        this(CronSourceConfigDefinition.config(), originals);
    }

    public CronSourceConnectorConfig(ConfigDef configDef, Map<?, ?> originals){
        super(configDef,originals);
        this.topic = Optional.ofNullable(getString(CronSourceConfigDefinition.TOPIC)).orElseThrow(()-> new IllegalArgumentException(CronSourceConfigDefinition.TOPIC + CANNOT_BE_FOUND_IN_MAP_CONFIGURATION));
        this.jobIds  = getList(CronSourceConfigDefinition.JOBS);


        jobIds.stream().map(id->{
            // example: * * * * ? *|http://www.example.com
            // example: * * * * ? *|http://www.example.com::PUT
            // example: * * * * ? *|http://www.example.com::POST::mysuperbodycontent
            String cronKey = id + ".cron";
            String cron = (String) originals.get(cronKey);
            Preconditions.checkNotNull(cron,"'"+cronKey+"' in settings is missing");
            String urlKey = id + ".url";
            String url = (String) originals.get(urlKey);
            Preconditions.checkNotNull(url,"'"+urlKey+"' in settings is missing");

            Optional<String> methodAsString = Optional.ofNullable((String) originals.get(id+".method"));
            HttpRequest.Method method = HttpRequest.Method.valueOf(methodAsString.orElse("GET"));
            Optional<String> bodyAsString = Optional.ofNullable((String) originals.get(id+".body"));
            try {
                CronExpression cronExpression = new CronExpression(cron);
                HttpRequest httpRequest = new HttpRequest(url, method);
                bodyAsString.ifPresent(httpRequest::setBodyAsString);
                return Pair.of(cronExpression,httpRequest);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
    }




    public String getTopic() {
        return topic;
    }

    public List<String> getJobs() {
        return jobIds;
    }
}
