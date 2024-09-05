package io.github.clescot.kafka.connect.http.source;

import com.google.common.collect.Lists;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class CronSourceConnectorConfig extends AbstractConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(CronSourceConnectorConfig.class);
    public static final String CANNOT_BE_FOUND_IN_MAP_CONFIGURATION = " cannot be found in map configuration";
    private final String topic;
    private List<Pair<CronExpression, HttpRequest>> jobs;


    public CronSourceConnectorConfig(Map<?, ?> originals) {
        this(CronSourceConfigDefinition.config(), originals);
    }

    public CronSourceConnectorConfig(ConfigDef configDef, Map<?, ?> originals){
        super(configDef,originals);
        this.topic = Optional.ofNullable(getString(CronSourceConfigDefinition.TOPIC)).orElseThrow(()-> new IllegalArgumentException(CronSourceConfigDefinition.TOPIC + CANNOT_BE_FOUND_IN_MAP_CONFIGURATION));
        this.jobs = getList(CronSourceConfigDefinition.JOBS).stream().map(string->{
            ArrayList<String> parts = Lists.newArrayList(string.split("\\|"));
            String cron = parts.get(0);
            String request = parts.get(1);
            List<String> requestParts = Lists.newArrayList(request.split("::"));
            try {
                CronExpression cronExpression = new CronExpression(cron);
                String url = requestParts.get(0);
                HttpRequest.Method method = null;
                if(requestParts.size()>=2) {
                    method = HttpRequest.Method.valueOf(requestParts.get(1));
                }
                HttpRequest httpRequest = new HttpRequest(url, Optional.ofNullable(method).orElse(HttpRequest.Method.GET));
                if(requestParts.size()==3) {
                    String body = requestParts.get(2);
                    httpRequest.setBodyAsString(body);
                }
                return Pair.of(cronExpression,httpRequest);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
    }




    public String getTopic() {
        return topic;
    }

    public List<Pair<CronExpression, HttpRequest>> getJobs() {
        return jobs;
    }
}
