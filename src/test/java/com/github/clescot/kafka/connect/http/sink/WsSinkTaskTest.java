package com.github.clescot.kafka.connect.http.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.clescot.kafka.connect.http.ConfigConstants;
import com.github.clescot.kafka.connect.http.HttpRequest;
import com.github.clescot.kafka.connect.http.QueueFactory;
import com.github.clescot.kafka.connect.http.sink.client.HttpClient;
import com.github.clescot.kafka.connect.http.source.HttpExchange;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.jetbrains.annotations.NotNull;
import org.json.JSONException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.skyscreamer.jsonassert.Customization;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.comparator.CustomComparator;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.clescot.kafka.connect.http.sink.WsSinkConfigDefinition.PUBLISH_TO_IN_MEMORY_QUEUE;
import static com.github.clescot.kafka.connect.http.sink.WsSinkConfigDefinition.STATIC_REQUEST_HEADER_NAMES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class WsSinkTaskTest {



    @BeforeEach
    public void setUp(){
        QueueFactory.clearRegistrations();
    }

    @Test
    public void test_start_with_queue_name(){
        WsSinkTask wsSinkTask = getWsSinkTask();
        Map<String,String> settings = Maps.newHashMap();
        settings.put(ConfigConstants.QUEUE_NAME,"dummyQueueName");
        wsSinkTask.start(settings);
    }

    @Test
    public void test_start_with_static_request_headers(){
        WsSinkTask wsSinkTask = getWsSinkTask();
        Map<String,String> settings = Maps.newHashMap();
        settings.put(STATIC_REQUEST_HEADER_NAMES,"param1,param2");
        settings.put("param1","value1");
        settings.put("param2","value2");
        wsSinkTask.start(settings);
    }

    @Test
    public void test_start_with_static_request_headers_without_required_parameters(){
        Assertions.assertThrows(NullPointerException.class,()->{
            WsSinkTask wsSinkTask = new WsSinkTask();
            Map<String,String> settings = Maps.newHashMap();
            settings.put(STATIC_REQUEST_HEADER_NAMES,"param1,param2");
            wsSinkTask.start(settings);
        });

    }


    @Test
    public void test_start_no_settings(){
        WsSinkTask wsSinkTask = getWsSinkTask();
        wsSinkTask.start(Maps.newHashMap());
    }


    @Test
    public void test_put_add_static_headers(){
        WsSinkTask wsSinkTask = getWsSinkTask();
        Map<String,String> settings = Maps.newHashMap();
        settings.put(STATIC_REQUEST_HEADER_NAMES,"param1,param2");
        settings.put("param1","value1");
        settings.put("param2","value2");
        wsSinkTask.start(settings);
        HttpClient httpClient = mock(HttpClient.class);
        HttpExchange dummyHttpExchange = getDummyHttpExchange();
        when(httpClient.call(any(HttpRequest.class))).thenReturn(dummyHttpExchange);
        wsSinkTask.setHttpClient(httpClient);
        List<SinkRecord> records = Lists.newArrayList();
        List<Header> headers = Lists.newArrayList();
        SinkRecord sinkRecord = new SinkRecord("myTopic",0, Schema.STRING_SCHEMA,"key",Schema.STRING_SCHEMA,getDummyHttpRequestAsString(),-1,System.currentTimeMillis(), TimestampType.CREATE_TIME,headers);
        records.add(sinkRecord);
        wsSinkTask.put(records);
        ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(httpClient,times(1)).call(captor.capture());
        HttpRequest enhancedRecordBeforeHttpCall = captor.getValue();
        assertThat(enhancedRecordBeforeHttpCall.getHeaders().size()==sinkRecord.headers().size()+wsSinkTask.getStaticRequestHeaders().size());
        assertThat(enhancedRecordBeforeHttpCall.getHeaders()).contains(Map.entry("param1",Lists.newArrayList("value1")));
        assertThat(enhancedRecordBeforeHttpCall.getHeaders()).contains(Map.entry("param2",Lists.newArrayList("value2")));
    }

    @Test
    public void test_put_nominal_case(){
        //given
        WsSinkTask wsSinkTask = getWsSinkTask();
        Map<String,String> settings = Maps.newHashMap();
        wsSinkTask.start(settings);

        //mock httpClient
        HttpClient httpClient = mock(HttpClient.class);
        HttpExchange dummyHttpExchange = getDummyHttpExchange();
        when(httpClient.call(any(HttpRequest.class))).thenReturn(dummyHttpExchange);
        wsSinkTask.setHttpClient(httpClient);

        //mock queue
        Queue<HttpExchange> dummyQueue = mock(Queue.class);
        wsSinkTask.setQueue(dummyQueue);

        //init sinkRecord
        List<SinkRecord> records = Lists.newArrayList();
        List<Header> headers = Lists.newArrayList();
        SinkRecord sinkRecord = new SinkRecord("myTopic",0, Schema.STRING_SCHEMA,"key",Schema.STRING_SCHEMA,getDummyHttpRequestAsString(),-1,System.currentTimeMillis(), TimestampType.CREATE_TIME,headers);
        records.add(sinkRecord);

        //when
        wsSinkTask.put(records);

        //then

        //no additional headers added
        ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(httpClient,times(1)).call(captor.capture());
        HttpRequest enhancedRecordBeforeHttpCall = captor.getValue();
        assertThat(enhancedRecordBeforeHttpCall.getHeaders().size()==sinkRecord.headers().size());

        //no records are published into the in memory queue by default
        verify(dummyQueue,never()).offer(any(HttpExchange.class));
    }

    @Test
    public void test_put_with_publish_to_in_memory_queue_without_consumer(){
        //given
        WsSinkTask wsSinkTask = getWsSinkTask();
        Map<String,String> settings = Maps.newHashMap();
        settings.put(PUBLISH_TO_IN_MEMORY_QUEUE,"true");
        wsSinkTask.start(settings);

        //mock httpClient
        HttpClient httpClient = mock(HttpClient.class);
        HttpExchange dummyHttpExchange = getDummyHttpExchange();
        when(httpClient.call(any(HttpRequest.class))).thenReturn(dummyHttpExchange);
        wsSinkTask.setHttpClient(httpClient);

        //mock queue
        Queue<HttpExchange> dummyQueue = mock(Queue.class);
        wsSinkTask.setQueue(dummyQueue);

        //init sinkRecord
        List<SinkRecord> records = Lists.newArrayList();
        List<Header> headers = Lists.newArrayList();
        SinkRecord sinkRecord = new SinkRecord("myTopic",0, Schema.STRING_SCHEMA,"key",Schema.STRING_SCHEMA,"myValue",-1,System.currentTimeMillis(), TimestampType.CREATE_TIME,headers);
        records.add(sinkRecord);

        //when
        //then
        Assertions.assertThrows(IllegalArgumentException.class,
                ()->wsSinkTask.put(records));

    }

    @NotNull
    private static WsSinkTask getWsSinkTask() {
        WsSinkTask wsSinkTask = new WsSinkTask();
        ErrantRecordReporter errantRecordReporter = mock(ErrantRecordReporter.class);
        SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
        when(sinkTaskContext.errantRecordReporter()).thenReturn(errantRecordReporter);
        wsSinkTask.initialize(sinkTaskContext);
        return wsSinkTask;
    }


    @Test
    public void test_put_with_publish_in_memory_set_to_false(){
        WsSinkTask wsSinkTask = getWsSinkTask();
        Map<String,String> settings = Maps.newHashMap();
        settings.put(PUBLISH_TO_IN_MEMORY_QUEUE,"false");
        wsSinkTask.start(settings);
        HttpClient httpClient = mock(HttpClient.class);
        HttpExchange dummyHttpExchange = getDummyHttpExchange();
        when(httpClient.call(any(HttpRequest.class))).thenReturn(dummyHttpExchange);
        wsSinkTask.setHttpClient(httpClient);
        Queue<HttpExchange> queue = mock(Queue.class);
        wsSinkTask.setQueue(queue);
        List<SinkRecord> records = Lists.newArrayList();
        List<Header> headers = Lists.newArrayList();
        SinkRecord sinkRecord = new SinkRecord("myTopic",0, Schema.STRING_SCHEMA,"key",Schema.STRING_SCHEMA,getDummyHttpRequestAsString(),-1,System.currentTimeMillis(), TimestampType.CREATE_TIME,headers);
        records.add(sinkRecord);
        wsSinkTask.put(records);
        verify(httpClient,times(1)).call(any(HttpRequest.class));
        verify(queue,never()).offer(any(HttpExchange.class));
    }

    @Test
    public void test_put_with_publish_to_in_memory_queue_set_to_true_with_a_consumer(){

        //given
        WsSinkTask wsSinkTask = getWsSinkTask();
        Map<String,String> settings = Maps.newHashMap();
        settings.put(PUBLISH_TO_IN_MEMORY_QUEUE,"true");
        QueueFactory.registerConsumerForQueue(QueueFactory.DEFAULT_QUEUE_NAME);
        wsSinkTask.start(settings);
        HttpClient httpClient = mock(HttpClient.class);
        HttpExchange dummyHttpExchange = getDummyHttpExchange();
        when(httpClient.call(any(HttpRequest.class))).thenReturn(dummyHttpExchange);
        wsSinkTask.setHttpClient(httpClient);
        Queue<HttpExchange> queue = mock(Queue.class);
        wsSinkTask.setQueue(queue);
        List<SinkRecord> records = Lists.newArrayList();
        List<Header> headers = Lists.newArrayList();
        SinkRecord sinkRecord = new SinkRecord("myTopic",0, Schema.STRING_SCHEMA,"key",Schema.STRING_SCHEMA,getDummyHttpRequestAsString(),-1,System.currentTimeMillis(), TimestampType.CREATE_TIME,headers);
        records.add(sinkRecord);
        //when
        wsSinkTask.put(records);

        //then
        verify(httpClient,times(1)).call(any(HttpRequest.class));
        verify(queue,times(1)).offer(any(HttpExchange.class));
    }


    @Test
    public void test_http_exchange_json_serialization() throws JsonProcessingException, JSONException {
        HttpExchange dummyHttpExchange = getDummyHttpExchange();
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        String httpExchangeAsString = objectMapper.writeValueAsString(dummyHttpExchange);
        String expectedJSON = "" +
                " {\n" +
                "  \"responseHeaders\": {\n" +
                "    \"Content-Type\": \"application/json\"\n" +
                "  },\n" +
                "  \"statusCode\": 200,\n" +
                "  \"statusMessage\": \"OK\",\n" +
                "  \"responseBody\": \"\",\n" +
                "  \"attempts\": 1,\n" +
                "  \"success\": true,\n" +
                "  \"httpRequest\": {\n" +
                "    \"requestId\": null,\n" +
                "    \"correlationId\": null,\n" +
                "    \"timeoutInMs\": null,\n" +
                "    \"retries\": null,\n" +
                "    \"retryDelayInMs\": null,\n" +
                "    \"retryMaxDelayInMs\": null,\n" +
                "    \"retryDelayFactor\": null,\n" +
                "    \"retryJitter\": null,\n" +
                "    \"url\": \"http://www.titi.com\",\n" +
                "    \"headers\": {\n" +
                "      \"X-dummy\": [\n" +
                "        \"blabla\"\n" +
                "      ]\n" +
                "    },\n" +
                "    \"method\": \"GET\",\n" +
                "    \"bodyAsString\": \"stuff\",\n" +
                "    \"bodyAsByteArray\": \"\",\n" +
                "    \"bodyAsMultipart\": [],\n" +
                "    \"bodyType\": \"STRING\"\n" +
                "  }\n" +
                "}";

        JSONAssert.assertEquals(expectedJSON, httpExchangeAsString,
                new CustomComparator(JSONCompareMode.LENIENT,
                        new Customization("moment", (o1, o2) -> true),
                        new Customization("durationInMillis", (o1, o2) -> true)
                ));


    }


    private HttpExchange getDummyHttpExchange() {
        Map<String, List<String>> requestHeaders = Maps.newHashMap();
        requestHeaders.put("X-dummy",Lists.newArrayList("blabla"));
        Map<String, String> responseHeaders = Maps.newHashMap();
        responseHeaders.put("Content-Type","application/json");
        HttpRequest httpRequest = new HttpRequest("http://www.titi.com","GET","stuff",null,null);
        httpRequest.setHeaders(requestHeaders);
        return new HttpExchange(
                httpRequest,
                200,
                "OK",
                responseHeaders,
                "",
                245L,
                OffsetDateTime.now(ZoneId.of("UTC")),
                new AtomicInteger(1),
                true
        );
    }


    private String getDummyHttpRequestAsString(){
        return "{\n" +
        "  \"requestId\": null,\n" +
                "  \"correlationId\": null,\n" +
                "  \"timeoutInMs\": 0,\n" +
                "  \"retries\": 0,\n" +
                "  \"retryDelayInMs\": 0,\n" +
                "  \"retryMaxDelayInMs\": 0,\n" +
                "  \"retryDelayFactor\": 0.0,\n" +
                "  \"retryJitter\": 0,\n" +
                "  \"url\": \"http://www.stuff.com\",\n" +
                "  \"headers\": {},\n" +
                "  \"method\": \"GET\",\n" +
                "  \"bodyAsString\": \"stuff\",\n" +
                "  \"bodyAsByteArray\": null,\n" +
                "  \"bodyAsMultipart\": null,\n" +
                "  \"bodyType\": \"STRING\"\n" +
                "}";
    }

}