package com.github.clescot.kafka.connect.http.source;

import com.github.clescot.kafka.connect.http.HttpExchange;
import com.github.clescot.kafka.connect.http.QueueFactory;
import com.github.clescot.kafka.connect.http.sink.VersionUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static com.github.clescot.kafka.connect.http.HttpExchange.*;

public class HttpSourceTask extends SourceTask {

    private Queue<HttpExchange> queue;
    private String queueName;
    private HttpSourceConnectorConfig sourceConfig;
    private final static Logger LOGGER = LoggerFactory.getLogger(HttpSourceTask.class);



    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }

    @Override
    public void start(Map<String, String> taskConfig) {
        Preconditions.checkNotNull(taskConfig, "taskConfig cannot be null");
        this.sourceConfig = new HttpSourceConnectorConfig(taskConfig);
        this.queueName = sourceConfig.getQueueName();
        queue = QueueFactory.getQueue(queueName);
        QueueFactory.registerConsumerForQueue(queueName);
    }

    @Override
    public List<SourceRecord> poll() {
        List<SourceRecord> records = Lists.newArrayList();
        while (queue.peek() != null) {
            HttpExchange httpExchange = queue.poll();
            LOGGER.debug("received httpExchange from queue '{}':{}",queueName,httpExchange);
            if(httpExchange!=null){
                SourceRecord sourceRecord = toSourceRecord(httpExchange);
                LOGGER.debug("send ack to queue '{}' with source record :{}",queueName,sourceRecord);
                records.add(sourceRecord);
            }
        }

        return records;
    }


    private SourceRecord toSourceRecord(HttpExchange httpExchange){
        //sourcePartition and sourceOffset are useful to track data consumption from source
        //but it is useless in the in memory queue context
        Map<String, ?> sourcePartition = Maps.newHashMap();
        Map<String, ?> sourceOffset= Maps.newHashMap();
        Struct struct = httpExchange.toStruct();

        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                httpExchange.isSuccess()? sourceConfig.getSuccessTopic(): sourceConfig.getErrorsTopic(),
                struct.schema(),
                struct
        );
    }




    @Override
    public void stop() {

    }
}
