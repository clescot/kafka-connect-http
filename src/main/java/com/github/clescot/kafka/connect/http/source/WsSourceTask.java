package com.github.clescot.kafka.connect.http.source;

import com.github.clescot.kafka.connect.http.QueueFactory;
import com.github.clescot.kafka.connect.http.sink.utils.VersionUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TransferQueue;

public class WsSourceTask extends SourceTask {

    public static final String DURATION_IN_MILLIS = "durationInMillis";
    public static final String MOMENT = "moment";
    public static final String ATTEMPTS = "attempts";
    public static final String CORRELATION_ID = "correlationId";
    public static final String REQUEST_ID = "requestId";
    public static final String REQUEST_URI = "requestUri";
    public static final String METHOD = "method";
    public static final String REQUEST_HEADERS = "requestHeaders";
    public static final String REQUEST_BODY = "requestBody";
    public static final String STATUS_CODE = "statusCode";
    public static final String STATUS_MESSAGE = "statusMessage";
    public static final String RESPONSE_HEADERS = "responseHeaders";
    public static final String RESPONSE_BODY = "responseBody";
    private static TransferQueue<Acknowledgement> queue;
    private AckConfig ackConfig;

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }

    @Override
    public void start(Map<String, String> taskConfig) {
        Preconditions.checkNotNull(taskConfig, "taskConfig cannot be null");
        this.queue = QueueFactory.getQueue();
        this.ackConfig = new AckConfig(taskConfig);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Acknowledgement acknowledgement = queue.take();
        SourceRecord sourceRecord = transformAcknowledgment(acknowledgement);
        return Lists.newArrayList(sourceRecord);
    }


    private SourceRecord transformAcknowledgment(Acknowledgement acknowledgement){
        Map<String, ?> sourcePartition = Maps.newHashMap();
        Map<String, ?> sourceOffset= Maps.newHashMap();
        Struct struct = new Struct(getSchema());

        SourceRecord sourceRecord = new SourceRecord(sourcePartition,sourceOffset,ackConfig.getAckTopic(),struct.schema(),struct);
        return sourceRecord;
    }

    private Schema getSchema() {
        return SchemaBuilder
                .struct()
                //ack fields
                .field(DURATION_IN_MILLIS,Schema.INT64_SCHEMA)
                .field(MOMENT,Schema.STRING_SCHEMA)
                .field(ATTEMPTS,Schema.INT32_SCHEMA)
                //request fields
                .field(CORRELATION_ID,Schema.STRING_SCHEMA)
                .field(REQUEST_ID,Schema.STRING_SCHEMA)
                .field(REQUEST_URI,Schema.STRING_SCHEMA)
                .field(METHOD,Schema.STRING_SCHEMA)
                .field(REQUEST_HEADERS,
                        SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA))
                .field(REQUEST_BODY,Schema.STRING_SCHEMA)
                // response fields
                .field(STATUS_CODE,Schema.INT16_SCHEMA)
                .field(STATUS_MESSAGE,Schema.STRING_SCHEMA)
                .field(RESPONSE_HEADERS,
                    SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA))
                .field(RESPONSE_BODY,Schema.STRING_SCHEMA)
                .valueSchema();

    }


    @Override
    public void stop() {

    }
}
