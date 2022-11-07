package com.github.clescot.kafka.connect.http.sink;

import com.github.clescot.kafka.connect.http.sink.utils.HeaderImpl;
import com.github.clescot.kafka.connect.http.sink.config.AckConfig;
import com.github.clescot.kafka.connect.http.sink.service.AckSender;
import com.google.common.collect.Maps;
import io.confluent.connect.avro.AvroConverter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.assertj.core.util.Lists;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.asynchttpclient.uri.Uri;
import org.junit.AssumptionViolatedException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.Stopwatch;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.github.clescot.kafka.connect.http.sink.WsSinkTask.*;
import static com.github.clescot.kafka.connect.http.sink.config.ConfigConstants.*;
import static com.github.clescot.kafka.connect.http.sink.service.WsCaller.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Enclosed.class)
public class WsSinkTaskTest {

    final static Logger LOGGER = LoggerFactory.getLogger(WsSinkTaskTest.class);
    public static final String VALID_ACK_SCHEMA = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"downStream\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"correlationId\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"requestId\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"statusCode\",\n" +
            "      \"type\": \"int\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"statusMessage\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\"name\": \"responseHeaders\",\n" +
            "      \"type\": {\n" +
            "        \"type\":\"array\",\n" +
            "        \"items\": \"string\"\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"responseBody\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"method\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"requestUri\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\"name\": \"requestHeaders\",\n" +
            "      \"type\": {\n" +
            "        \"type\":\"array\",\n" +
            "        \"items\": \"string\"\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"requestBody\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"durationInMillis\",\n" +
            "      \"type\": \"long\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"moment\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"attempts\",\n" +
            "      \"type\": \"int\"\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    private static void logInfo(Description description, String status, long nanos) {
        String testName = description.getMethodName();
        LOGGER.info(String.format("Test %s %s, spent %d microseconds",
                testName, status, TimeUnit.NANOSECONDS.toMicros(nanos)));
    }



    public static class Test_Start{

        @Before
        public void setup(){
            AckSender.clearCurrentInstance();
        }

        @Test(expected = IllegalArgumentException.class)
        public void test_empty_map(){
            HashMap<String, String> vars = com.google.common.collect.Maps.newHashMap();
            WsSinkTask wsSinkTask = new WsSinkTask();
            wsSinkTask.start(Maps.newHashMap());
        }

        @Test(expected = NullPointerException.class)
        public void test_null_map(){
            HashMap<String, String> vars = com.google.common.collect.Maps.newHashMap();
            WsSinkTask wsSinkTask = new WsSinkTask();
            wsSinkTask.start(null);
        }
        @Test
        public void test_nominal_case(){
            HashMap<String, String> vars = com.google.common.collect.Maps.newHashMap();
            WsSinkTask wsSinkTask = new WsSinkTask();
            HashMap<String, String> taskConfig = Maps.newHashMap();
            taskConfig.put(TARGET_BOOTSTRAP_SERVER,"localhost:9092");
            taskConfig.put(TARGET_SCHEMA_REGISTRY,"localhost:8081");
            taskConfig.put(PRODUCER_CLIENT_ID,"fake.client.id");
            taskConfig.put(ACK_TOPIC,"fake.ack.topic");
            taskConfig.put(ACK_SCHEMA,"{\n" +
                    "    \"namespace\": \"com.fake.namespace\",\n" +
                    "    \"name\": \"Test\",\n" +
                    "    \"doc\": \"test doc\",\n" +
                    "    \"type\": \"record\",\n" +
                    "    \"fields\": [\n" +
                    "        {\"name\": \"name\", \"type\": \"string\"},\n" +
                    "        {\"name\": \"id\", \"type\": \"int\"}\n" +
                    "    ]\n" +
                    "}");
            wsSinkTask.start(taskConfig);
        }


    }

    public static class Test_put{
        @Before
        public void setup(){
            AckSender.clearCurrentInstance();
        }


        @Rule
        public Stopwatch stopwatch = new Stopwatch() {
            @Override
            protected void succeeded(long nanos, Description description) {
                logInfo(description, "succeeded", nanos);
            }

            @Override
            protected void failed(long nanos, Throwable e, Description description) {
                logInfo(description, "failed", nanos);
            }

            @Override
            protected void skipped(long nanos, AssumptionViolatedException e, Description description) {
                logInfo(description, "skipped", nanos);
            }

            @Override
            protected void finished(long nanos, Description description) {
                logInfo(description, "finished", nanos);
            }
        };

        @Before
        public void setUp(){
            AckSender.clearCurrentInstance();
        }


        @Test
        public void test_empty_records(){
            HashMap<String, String> vars = com.google.common.collect.Maps.newHashMap();
            WsSinkTask wsSinkTask = new WsSinkTask();
            HashMap<String, String> taskConfig = Maps.newHashMap();
            taskConfig.put(TARGET_BOOTSTRAP_SERVER,"localhost:9092");
            taskConfig.put(TARGET_SCHEMA_REGISTRY,"localhost:8081");
            taskConfig.put(PRODUCER_CLIENT_ID,"fake.client.id");
            taskConfig.put(ACK_TOPIC,"fake.ack.topic");
            taskConfig.put(ACK_SCHEMA,"{\n" +
                    "    \"namespace\": \"com.fake.namespace\",\n" +
                    "    \"name\": \"Test\",\n" +
                    "    \"doc\": \"test doc\",\n" +
                    "    \"type\": \"record\",\n" +
                    "    \"fields\": [\n" +
                    "        {\"name\": \"name\", \"type\": \"string\"},\n" +
                    "        {\"name\": \"id\", \"type\": \"int\"}\n" +
                    "    ]\n" +
                    "}");
            wsSinkTask.start(taskConfig);
            wsSinkTask.put(Lists.newArrayList());
        }

        @Test
        public void test_one_record_ok() throws ExecutionException, InterruptedException, URISyntaxException {
            HashMap<String, String> vars = com.google.common.collect.Maps.newHashMap();
            //given
            WsSinkTask wsSinkTask = new WsSinkTask();

            HashMap<String, String> taskConfig = Maps.newHashMap();
            taskConfig.put(TARGET_BOOTSTRAP_SERVER,"localhost:9092");
            taskConfig.put(TARGET_SCHEMA_REGISTRY,"localhost:8081");
            taskConfig.put(PRODUCER_CLIENT_ID,"fake.client.id");
            taskConfig.put(ACK_TOPIC,"fake.ack.topic");
            taskConfig.put(ACK_SCHEMA, VALID_ACK_SCHEMA);
            wsSinkTask.start(taskConfig);

            //mock response
            AsyncHttpClient asyncHttpClient = mock(AsyncHttpClient.class);
            ListenableFuture<Object> futureResponse = mock(ListenableFuture.class);
            Response response = mock(Response.class);
            when(response.getResponseBody()).thenReturn("body");
            when(response.getStatusCode()).thenReturn(200);
            when(response.getStatusText()).thenReturn("OK");
            Uri uri = new Uri("http",null,"fakeHost",8080,"/toto","param1=3","#4");
            when(response.getUri()).thenReturn(uri);


            when(futureResponse.get()).thenReturn(response);
            when(asyncHttpClient.executeRequest(any(Request.class),any())).thenReturn(futureResponse);
            wsSinkTask.getWsCaller().setAsyncHttpClient(asyncHttpClient);

            AvroConverter avroConverter = mock(AvroConverter.class);
            when(avroConverter.fromConnectData(anyString(),any(Schema.class),any())).thenReturn("ok".getBytes());
            wsSinkTask.getAckSender().setValueConverter(avroConverter);

            //build sinkRecord
            Headers headers = new ConnectHeaders();
            Header url = new HeaderImpl("ws-url",Schema.STRING_SCHEMA,"http://localhost:8087");
            headers.add(url);
            Header method = new HeaderImpl("ws-method",Schema.STRING_SCHEMA,"PUT");
            headers.add(method);
            Header correlationId = new HeaderImpl("ws-correlation-id",Schema.STRING_SCHEMA,"qsdfsdfsdf654d5zgt5T");
            headers.add(correlationId);
            ArrayList<SinkRecord> records = Lists.newArrayList();
            Schema httpCall = new SchemaBuilder(Schema.Type.STRUCT)
            .field("body",Schema.STRING_SCHEMA).build();

            Struct struct = new Struct(httpCall);
            struct.put("body","myBody");

            AckConfig config = new AckConfig(taskConfig);
            SinkRecord sinkRecord = new SinkRecord("topic",1,null,null,config.getAckSchema(),struct,1,1L, TimestampType.CREATE_TIME,headers);
            records.add(sinkRecord);
            KafkaProducer<String, byte[]> producer = mock(KafkaProducer.class);
            RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topic",0), 0L,1L,1L,645465L,12,12);
            Future<RecordMetadata> recordMetadataFuture = CompletableFuture.supplyAsync(()-> recordMetadata);
            when(producer.send(any())).thenReturn(recordMetadataFuture);
            wsSinkTask.getAckSender().setProducer(producer);

            //when
            wsSinkTask.put(records);

            //then
        }

        @Test
        public void test_throttling() throws ExecutionException, InterruptedException {
            HashMap<String, String> vars = com.google.common.collect.Maps.newHashMap();
            //given
            WsSinkTask wsSinkTask = new WsSinkTask();

            HashMap<String, String> taskConfig = Maps.newHashMap();
            taskConfig.put(TARGET_BOOTSTRAP_SERVER,"localhost:9092");
            taskConfig.put(TARGET_SCHEMA_REGISTRY,"localhost:8081");
            taskConfig.put(PRODUCER_CLIENT_ID,"fake.client.id");
            taskConfig.put(ACK_TOPIC,"fake.ack.topic");
            taskConfig.put(ACK_SCHEMA,VALID_ACK_SCHEMA);
            taskConfig.put(HTTP_RATE_LIMIT_PER_SECOND,"4");
            taskConfig.put(HTTP_MAX_CONNECTIONS,"2");
            taskConfig.put(HTTP_MAX_WAIT_MS,"2000");

            wsSinkTask.start(taskConfig);

            //mock response
            AsyncHttpClient asyncHttpClient = mock(AsyncHttpClient.class);
            ListenableFuture<Object> futureResponse = mock(ListenableFuture.class);
            Response response = mock(Response.class);
            when(response.getResponseBody()).thenReturn("body");
            when(response.getStatusCode()).thenReturn(500);
            when(response.getStatusText()).thenReturn("Internal Server Error");
            when(futureResponse.get()).thenReturn(response);
            when(asyncHttpClient.executeRequest(any(Request.class),any())).thenReturn(futureResponse);
            wsSinkTask.getWsCaller().setAsyncHttpClient(asyncHttpClient);

            AvroConverter avroConverter = mock(AvroConverter.class);
            when(avroConverter.fromConnectData(anyString(),any(Schema.class),any())).thenReturn("ok".getBytes());
            wsSinkTask.getAckSender().setValueConverter(avroConverter);

            //build sinkRecord
            Headers headers = new ConnectHeaders();
            Header url = new HeaderImpl("ws-url",Schema.STRING_SCHEMA,"http://localhost:8087");
            headers.add(url);
            Header method = new HeaderImpl("ws-method",Schema.STRING_SCHEMA,"PUT");
            headers.add(method);
            Header correlationId = new HeaderImpl("ws-"+ HEADER_X_CORRELATION_ID,Schema.STRING_SCHEMA,"qsdfsdfsdf654d5zgt5T");
            headers.add(correlationId);
            Header retries = new HeaderImpl("ws-"+WS_RETRIES,Schema.STRING_SCHEMA,"2");
            headers.add(retries);
            Header retryDelayInMs = new HeaderImpl("ws-"+WS_RETRY_DELAY_IN_MS,Schema.STRING_SCHEMA,"200");
            headers.add(retryDelayInMs);
            Header retryJitter = new HeaderImpl("ws-"+WS_RETRY_JITTER,Schema.STRING_SCHEMA,"100");
            headers.add(retryJitter);
            Header retryFactor = new HeaderImpl("ws-"+WS_RETRY_DELAY_FACTOR,Schema.STRING_SCHEMA,"1.1");
            headers.add(retryFactor);
            Header readTimeoutInMs = new HeaderImpl("ws-"+WS_READ_TIMEOUT_IN_MS,Schema.STRING_SCHEMA,"15000");
            headers.add(readTimeoutInMs);
            Header requestTimeoutInMs = new HeaderImpl("ws-"+WS_REQUEST_TIMEOUT_IN_MS,Schema.STRING_SCHEMA,"15000");
            headers.add(requestTimeoutInMs);
            Header httpSuccessCode = new HeaderImpl("ws-"+WS_SUCCESS_CODE,Schema.STRING_SCHEMA,"200");
            headers.add(httpSuccessCode);

            ArrayList<SinkRecord> records = Lists.newArrayList();
            Schema httpCall = new SchemaBuilder(Schema.Type.STRUCT)
                    .field("body",Schema.STRING_SCHEMA).build();

            Struct struct = new Struct(httpCall);
            struct.put("body","myBody");

            AckConfig config = new AckConfig(taskConfig);
            SinkRecord sinkRecord = new SinkRecord("topic",1,null,null,config.getAckSchema(),struct,1,1L, TimestampType.CREATE_TIME,headers);
            for (int i = 0; i < 6; i++) {
                records.add(sinkRecord);
            }
            KafkaProducer<String, byte[]> producer = mock(KafkaProducer.class);
            RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topic",0), 0L,1L,1L,645465L,12,12);
            Future<RecordMetadata> recordMetadataFuture = CompletableFuture.supplyAsync(()-> recordMetadata);
            when(producer.send(any())).thenReturn(recordMetadataFuture);
            wsSinkTask.getAckSender().setProducer(producer);

            //when
            wsSinkTask.put(records);
            long runtime = stopwatch.runtime(TimeUnit.MILLISECONDS);
            System.out.println("runtime="+runtime);
            assertThat(runtime).isGreaterThan(2000L);

        }



        @Test
        public void test_producer_close() throws ExecutionException, InterruptedException {

            //configure sink task
            HashMap<String, String> vars = com.google.common.collect.Maps.newHashMap();
            //given
            WsSinkTask wsSinkTask = new WsSinkTask();

            HashMap<String, String> taskConfig = Maps.newHashMap();
            taskConfig.put(TARGET_BOOTSTRAP_SERVER,"localhost:9092");
            taskConfig.put(TARGET_SCHEMA_REGISTRY,"localhost:8081");
            taskConfig.put(PRODUCER_CLIENT_ID,"fake.client.id");
            taskConfig.put(ACK_TOPIC,"fake.ack.topic");
            taskConfig.put(ACK_SCHEMA,VALID_ACK_SCHEMA);
            taskConfig.put(HTTP_RATE_LIMIT_PER_SECOND,"4");
            taskConfig.put(HTTP_MAX_CONNECTIONS,"2");
            taskConfig.put(HTTP_MAX_WAIT_MS,"2000");

            wsSinkTask.start(taskConfig);

            //mock kafka producer (and retryable wrapper)
            KafkaProducer<String, byte[]> producer = mock(KafkaProducer.class);
            RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topic",0), 0L,1L,1L,645465L,12,12);
            Future<RecordMetadata> recordMetadataFuture = CompletableFuture.supplyAsync(()-> recordMetadata);
            when(producer.send(any())).thenAnswer(new Answer<Object>() {
                private int count=0;
                @Override
                public Object answer(InvocationOnMock invocation) {
                    if (count<1) {
                        count++;
                        throw new IllegalStateException("I have failed.");
                    }

                    return recordMetadataFuture;
                }
            });
            wsSinkTask.getAckSender().setProducer(producer);
            wsSinkTask.getAckSender().setSupplier(()->producer);


            //mock response
            AsyncHttpClient asyncHttpClient = mock(AsyncHttpClient.class);
            ListenableFuture<Object> futureResponse = mock(ListenableFuture.class);
            Response response = mock(Response.class);
            when(response.getResponseBody()).thenReturn("body");
            when(response.getStatusCode()).thenReturn(500);
            when(response.getStatusText()).thenReturn("Internal Server Error");
            when(futureResponse.get()).thenReturn(response);
            when(asyncHttpClient.executeRequest(any(Request.class),any())).thenReturn(futureResponse);
            wsSinkTask.getWsCaller().setAsyncHttpClient(asyncHttpClient);

            AvroConverter avroConverter = mock(AvroConverter.class);
            when(avroConverter.fromConnectData(anyString(),any(Schema.class),any())).thenReturn("ok".getBytes());
            wsSinkTask.getAckSender().setValueConverter(avroConverter);

            //build sinkRecord
            Headers headers = new ConnectHeaders();
            Header url = new HeaderImpl("ws-url",Schema.STRING_SCHEMA,"http://localhost:8087");
            headers.add(url);
            Header method = new HeaderImpl("ws-method",Schema.STRING_SCHEMA,"PUT");
            headers.add(method);
            Header correlationId = new HeaderImpl("ws-"+ HEADER_X_CORRELATION_ID,Schema.STRING_SCHEMA,"qsdfsdfsdf654d5zgt5T");
            headers.add(correlationId);
            Header retries = new HeaderImpl("ws-"+WS_RETRIES,Schema.STRING_SCHEMA,"2");
            headers.add(retries);
            Header retryDelayInMs = new HeaderImpl("ws-"+WS_RETRY_DELAY_IN_MS,Schema.STRING_SCHEMA,"200");
            headers.add(retryDelayInMs);
            Header retryJitter = new HeaderImpl("ws-"+WS_RETRY_JITTER,Schema.STRING_SCHEMA,"100");
            headers.add(retryJitter);
            Header retryFactor = new HeaderImpl("ws-"+WS_RETRY_DELAY_FACTOR,Schema.STRING_SCHEMA,"1.1");
            headers.add(retryFactor);
            Header readTimeoutInMs = new HeaderImpl("ws-"+WS_READ_TIMEOUT_IN_MS,Schema.STRING_SCHEMA,"500");
            headers.add(readTimeoutInMs);
            Header requestTimeoutInMs = new HeaderImpl("ws-"+WS_REQUEST_TIMEOUT_IN_MS,Schema.STRING_SCHEMA,"500");
            headers.add(requestTimeoutInMs);
            Header httpSuccessCode = new HeaderImpl("ws-"+WS_SUCCESS_CODE,Schema.STRING_SCHEMA,"200");
            headers.add(httpSuccessCode);

            ArrayList<SinkRecord> records = Lists.newArrayList();
            Schema httpCall = new SchemaBuilder(Schema.Type.STRUCT)
                    .field("body",Schema.STRING_SCHEMA).build();

            Struct struct = new Struct(httpCall);
            struct.put("body","myBody");

            AckConfig config = new AckConfig(taskConfig);
            SinkRecord sinkRecord = new SinkRecord("topic",1,null,null,config.getAckSchema(),struct,1,1L, TimestampType.CREATE_TIME,headers);
            for (int i = 0; i < 5; i++) {
                records.add(sinkRecord);
            }
            //when
            wsSinkTask.put(records);

        }

        @Test
        public void test_throttling_with_star_http_success_code() throws ExecutionException, InterruptedException {
            HashMap<String, String> vars = com.google.common.collect.Maps.newHashMap();
            //given
            WsSinkTask wsSinkTask = new WsSinkTask();

            HashMap<String, String> taskConfig = Maps.newHashMap();
            taskConfig.put(TARGET_BOOTSTRAP_SERVER,"localhost:9092");
            taskConfig.put(TARGET_SCHEMA_REGISTRY,"localhost:8081");
            taskConfig.put(PRODUCER_CLIENT_ID,"fake.client.id");
            taskConfig.put(ACK_TOPIC,"fake.ack.topic");
            taskConfig.put(ACK_SCHEMA,VALID_ACK_SCHEMA);
            taskConfig.put(HTTP_RATE_LIMIT_PER_SECOND,"4");
            taskConfig.put(HTTP_MAX_CONNECTIONS,"2");
            taskConfig.put(HTTP_MAX_WAIT_MS,"2000");


            wsSinkTask.start(taskConfig);

            //mock response
            AsyncHttpClient asyncHttpClient = mock(AsyncHttpClient.class);
            ListenableFuture<Object> futureResponse = mock(ListenableFuture.class);
            Response response = mock(Response.class);
            Uri uri = new Uri("http",null,"fakeHost",8080,"/toto","param1=3","#4");
            when(response.getUri()).thenReturn(uri);


            when(response.getResponseBody()).thenReturn("body");
            when(response.getStatusCode()).thenReturn(500);
            when(response.getStatusText()).thenReturn("Internal Server Error");
            when(futureResponse.get()).thenReturn(response);
            when(asyncHttpClient.executeRequest(any(Request.class),any())).thenReturn(futureResponse);
            wsSinkTask.getWsCaller().setAsyncHttpClient(asyncHttpClient);

            AvroConverter avroConverter = mock(AvroConverter.class);
            when(avroConverter.fromConnectData(anyString(),any(Schema.class),any())).thenReturn("ok".getBytes());
            wsSinkTask.getAckSender().setValueConverter(avroConverter);

            //build sinkRecord
            Headers headers = new ConnectHeaders();
            Header url = new HeaderImpl("ws-url",Schema.STRING_SCHEMA,"http://localhost:8087");
            headers.add(url);
            Header method = new HeaderImpl("ws-method",Schema.STRING_SCHEMA,"PUT");
            headers.add(method);
            Header correlationId = new HeaderImpl("ws-"+ HEADER_X_CORRELATION_ID,Schema.STRING_SCHEMA,"qsdfsdfsdf654d5zgt5T");
            headers.add(correlationId);
            Header retries = new HeaderImpl("ws-"+WS_RETRIES,Schema.STRING_SCHEMA,"2");
            headers.add(retries);
            Header retryDelayInMs = new HeaderImpl("ws-"+WS_RETRY_DELAY_IN_MS,Schema.STRING_SCHEMA,"2000");
            headers.add(retryDelayInMs);
            Header readTimeoutInMs = new HeaderImpl("ws-"+WS_READ_TIMEOUT_IN_MS,Schema.STRING_SCHEMA,"15000");
            headers.add(readTimeoutInMs);
            Header requestTimeoutInMs = new HeaderImpl("ws-"+WS_REQUEST_TIMEOUT_IN_MS,Schema.STRING_SCHEMA,"15000");
            headers.add(requestTimeoutInMs);
            Header httpSuccessCode = new HeaderImpl("ws-"+WS_SUCCESS_CODE,Schema.STRING_SCHEMA,".*");
            headers.add(httpSuccessCode);


            ArrayList<SinkRecord> records = Lists.newArrayList();
            Schema httpCall = new SchemaBuilder(Schema.Type.STRUCT)
                    .field("body",Schema.STRING_SCHEMA).build();

            Struct struct = new Struct(httpCall);
            struct.put("body","myBody");

            AckConfig config = new AckConfig(taskConfig);
            SinkRecord sinkRecord = new SinkRecord("topic",1,null,null,config.getAckSchema(),struct,1,1L, TimestampType.CREATE_TIME,headers);
            for (int i = 0; i < 6; i++) {
                records.add(sinkRecord);
            }
            KafkaProducer<String, byte[]> producer = mock(KafkaProducer.class);
            RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topic",0), 0L,1L,1L,645465L,12,12);
            Future<RecordMetadata> recordMetadataFuture = CompletableFuture.supplyAsync(()-> recordMetadata);
            when(producer.send(any())).thenReturn(recordMetadataFuture);
            wsSinkTask.getAckSender().setProducer(producer);

            //when
            wsSinkTask.put(records);
            long runtime = stopwatch.runtime(TimeUnit.MILLISECONDS);
            System.out.println("runtime="+runtime);

        }




        @Test
        public void test_one_record_ko_500() throws ExecutionException, InterruptedException {
            HashMap<String, String> vars = com.google.common.collect.Maps.newHashMap();
            WsSinkTask wsSinkTask = new WsSinkTask();

            HashMap<String, String> taskConfig = Maps.newHashMap();
            taskConfig.put(TARGET_BOOTSTRAP_SERVER,"localhost:9092");
            taskConfig.put(TARGET_SCHEMA_REGISTRY,"localhost:8081");
            taskConfig.put(PRODUCER_CLIENT_ID,"fake.client.id");
            taskConfig.put(ACK_TOPIC,"fake.ack.topic");
            taskConfig.put(ACK_SCHEMA,VALID_ACK_SCHEMA);
            wsSinkTask.start(taskConfig);

            //mock response
            AsyncHttpClient asyncHttpClient = mock(AsyncHttpClient.class);
            ListenableFuture<Object> futureResponse = mock(ListenableFuture.class);
            Response response = mock(Response.class);
            when(response.getResponseBody()).thenReturn("body");
            when(response.getStatusCode()).thenReturn(500);
            when(response.getStatusText()).thenReturn("Internal Server Error");
            when(futureResponse.get()).thenReturn(response);
            when(asyncHttpClient.executeRequest(any(Request.class),any())).thenReturn(futureResponse);
            wsSinkTask.getWsCaller().setAsyncHttpClient(asyncHttpClient);

            AvroConverter avroConverter = mock(AvroConverter.class);
            when(avroConverter.fromConnectData(anyString(),any(Schema.class),any())).thenReturn("ok".getBytes());
            wsSinkTask.getAckSender().setValueConverter(avroConverter);

            //build sinkRecord
            Headers headers = new ConnectHeaders();
            Header url = new HeaderImpl("ws-url",Schema.STRING_SCHEMA,"http://localhost:8087");
            headers.add(url);
            Header method = new HeaderImpl("ws-method",Schema.STRING_SCHEMA,"PUT");
            headers.add(method);
            Header correlationId = new HeaderImpl("ws-correlation-id",Schema.STRING_SCHEMA,"qsdfsdfsdf654d5zgt5T");
            headers.add(correlationId);
            Header retryDelay = new HeaderImpl("ws-retry-delay-in-ms",Schema.STRING_SCHEMA,"500");
            headers.add(retryDelay);
            Header retryDelayFactor = new HeaderImpl("ws-retry-delay-factor",Schema.STRING_SCHEMA,"1.1");
            headers.add(retryDelayFactor);
            Header retryJitter = new HeaderImpl("ws-retry-jitter",Schema.STRING_SCHEMA,"100");
            headers.add(retryJitter);
            ArrayList<SinkRecord> records = Lists.newArrayList();
            Schema httpCall = new SchemaBuilder(Schema.Type.STRUCT)
                    .field("body",Schema.STRING_SCHEMA).build();

            Struct struct = new Struct(httpCall);
            struct.put("body","myBody");

            AckConfig config = new AckConfig(taskConfig);
            SinkRecord sinkRecord = new SinkRecord("topic",1,null,null,config.getAckSchema(),struct,1,1L, TimestampType.CREATE_TIME,headers);
            records.add(sinkRecord);
            KafkaProducer<String, byte[]> producer = mock(KafkaProducer.class);
            RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topic",0), 0L,1L,1L,645465L,12,12);
            Future<RecordMetadata> recordMetadataFuture = CompletableFuture.supplyAsync(()-> recordMetadata);
            when(producer.send(any())).thenReturn(recordMetadataFuture);
            wsSinkTask.getAckSender().setProducer(producer);

            //test
            wsSinkTask.put(records);
        }




        @Test(expected = NullPointerException.class)
        public void test_null_records(){
            WsSinkTask wsSinkTask = new WsSinkTask();
            wsSinkTask.put(null);
        }
    }

    public static class Test_stop{
        @Before
        public void setup(){
            AckSender.clearCurrentInstance();
        }

        @Test
        public void test_nominal_case(){
            HashMap<String, String> vars = com.google.common.collect.Maps.newHashMap();
            WsSinkTask wsSinkTask = new WsSinkTask();
            wsSinkTask.stop();
        }
    }
}