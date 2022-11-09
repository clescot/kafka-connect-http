package com.github.clescot.kafka.connect.http.source;

import com.github.clescot.kafka.connect.http.QueueFactory;
import com.github.clescot.kafka.connect.http.sink.VersionUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Queue;

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
    public static final int ACK_SCHEMA_VERSION = 1;

    private static Queue<HttpExchange> queue;
    private WsSourceConnectorConfig sourceConfig;
    private final static Logger LOGGER = LoggerFactory.getLogger(WsSourceTask.class);

    private final static Schema ackSchema =getSchema();

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }

    @Override
    public void start(Map<String, String> taskConfig) {
        Preconditions.checkNotNull(taskConfig, "taskConfig cannot be null");
        this.sourceConfig = new WsSourceConnectorConfig(taskConfig);
        queue = QueueFactory.getQueue(sourceConfig.getQueueName());
        QueueFactory.registerConsumerForQueue(sourceConfig.getQueueName());
    }

    @Override
    public List<SourceRecord> poll() {
        List<SourceRecord> records = Lists.newArrayList();
        while (queue.peek() != null) {
            SourceRecord sourceRecord = toSourceRecord(queue.poll());
            LOGGER.debug("send ack with source record :{}",sourceRecord);
            records.add(sourceRecord);
        }

        return records;
    }


    private SourceRecord toSourceRecord(HttpExchange httpExchange){
        //sourcePartition and sourceOffset are useful to track data consumption from source
        //but it is useless in the in memory queue context
        Map<String, ?> sourcePartition = Maps.newHashMap();
        Map<String, ?> sourceOffset= Maps.newHashMap();
        Struct struct = new Struct(ackSchema);
        //ack fields
        struct.put(DURATION_IN_MILLIS,httpExchange.getDurationInMillis());
        struct.put(MOMENT,httpExchange.getMoment().format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
        struct.put(ATTEMPTS,httpExchange.getAttempts().intValue());
        //request fields
        struct.put(CORRELATION_ID,httpExchange.getCorrelationId());
        struct.put(REQUEST_ID,httpExchange.getRequestId());
        struct.put(REQUEST_URI,httpExchange.getRequestUri());
        struct.put(METHOD,httpExchange.getMethod());
        struct.put(REQUEST_HEADERS,httpExchange.getRequestHeaders());
        struct.put(REQUEST_BODY,httpExchange.getRequestBody());
        // response fields
        struct.put(STATUS_CODE,httpExchange.getStatusCode());
        struct.put(STATUS_MESSAGE,httpExchange.getStatusMessage());
        struct.put(RESPONSE_HEADERS,httpExchange.getResponseHeaders());
        struct.put(RESPONSE_BODY,httpExchange.getResponseBody());

        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                httpExchange.isSuccess()? sourceConfig.getSuccessTopic(): sourceConfig.getErrorsTopic(),
                struct.schema(),
                struct
        );
    }

    private static Schema getSchema() {
        return SchemaBuilder
                .struct()
                .name(HttpExchange.class.getName())
                .version(ACK_SCHEMA_VERSION)
                //ack fields
                .field(DURATION_IN_MILLIS, Schema.INT64_SCHEMA)
                .field(MOMENT, Schema.STRING_SCHEMA)
                .field(ATTEMPTS, Schema.INT32_SCHEMA)
                //request fields
                .field(CORRELATION_ID, Schema.STRING_SCHEMA)
                .field(REQUEST_ID, Schema.STRING_SCHEMA)
                .field(REQUEST_URI, Schema.STRING_SCHEMA)
                .field(METHOD, Schema.STRING_SCHEMA)
                .field(REQUEST_HEADERS,
                        SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA).build())
                .field(REQUEST_BODY, Schema.STRING_SCHEMA)
                // response fields
                .field(STATUS_CODE, Schema.INT32_SCHEMA)
                .field(STATUS_MESSAGE, Schema.STRING_SCHEMA)
                .field(RESPONSE_HEADERS,
                        SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA).build())
                .field(RESPONSE_BODY, Schema.STRING_SCHEMA)
                .schema();

    }


    @Override
    public void stop() {

    }
}
