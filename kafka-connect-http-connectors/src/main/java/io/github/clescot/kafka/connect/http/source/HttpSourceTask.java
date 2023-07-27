package io.github.clescot.kafka.connect.http.source;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.VersionUtils;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpExchangeAsStruct;
import io.github.clescot.kafka.connect.http.core.queue.KafkaRecord;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Queue;

public class HttpSourceTask extends SourceTask {

    private Queue<KafkaRecord> queue;
    private String queueName;
    private HttpSourceConnectorConfig sourceConfig;
    private final static Logger LOGGER = LoggerFactory.getLogger(HttpSourceTask.class);
    private static final VersionUtils VERSION_UTILS = new VersionUtils();


    @Override
    public String version() {
        return VERSION_UTILS.getVersion();
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
            KafkaRecord kafkaRecord = queue.poll();
            LOGGER.debug("received httpExchange from queue '{}':{}",queueName,kafkaRecord);
            if(kafkaRecord!=null){
                SourceRecord sourceRecord = toSourceRecord(kafkaRecord);
                LOGGER.debug("send ack to queue '{}' with source record :{}",queueName,sourceRecord);
                records.add(sourceRecord);
            }
        }

        return records;
    }


    private SourceRecord toSourceRecord(KafkaRecord kafkaRecord){
        //sourcePartition and sourceOffset are useful to track data consumption from source
        //but it is useless in the in memory queue context
        Map<String, ?> sourcePartition = Maps.newHashMap();
        Map<String, ?> sourceOffset= Maps.newHashMap();
        HttpExchange httpExchange = kafkaRecord.getHttpExchange();
        HttpExchangeAsStruct httpExchangeAsStruct = new HttpExchangeAsStruct(httpExchange);
        Struct struct = httpExchangeAsStruct.toStruct();
        LOGGER.debug("HttpSourcetask Struct received :{}",struct);
        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                httpExchange.isSuccess()? sourceConfig.getSuccessTopic(): sourceConfig.getErrorsTopic(),
                null,
                kafkaRecord.getSchemaKey(),
                kafkaRecord.getKey(),
                struct.schema(),
                struct,
                null,
                kafkaRecord.getHeaders()
        );
    }




    @Override
    public void stop() {

    }
}
