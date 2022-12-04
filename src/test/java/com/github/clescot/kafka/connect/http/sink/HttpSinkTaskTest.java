package com.github.clescot.kafka.connect.http.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.clescot.kafka.connect.http.*;
import com.github.clescot.kafka.connect.http.sink.client.ahc.AHCHttpClient;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.json.JSONException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
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

import static com.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class HttpSinkTaskTest {

    private static final String DUMMY_BODY = "stuff";
    private static final String DUMMY_URL = "http://www." + DUMMY_BODY + ".com";
    private static final String DUMMY_METHOD = "GET";
    private static final String DUMMY_BODY_TYPE = "STRING";
    public static final String CLIENT_TRUSTSTORE_JKS_FILENAME = "client_truststore.jks";
    public static final String CLIENT_TRUSTSTORE_JKS_PASSWORD = "Secret123!";
    public static final String JKS_STORE_TYPE = "jks";
    public static final String TRUSTSTORE_PKIX_ALGORITHM = "PKIX";
    @Mock
    ErrantRecordReporter errantRecordReporter;
    @Mock
    SinkTaskContext sinkTaskContext;

    @Mock
    Queue<HttpExchange> dummyQueue;

    @InjectMocks
    HttpSinkTask httpSinkTask;

    @BeforeEach
    public void setUp() {
        QueueFactory.clearRegistrations();
        MockitoAnnotations.openMocks(this);
        httpSinkTask.initialize(sinkTaskContext);
    }

    @Test
    public void test_start_with_queue_name() {
        Map<String, String> settings = Maps.newHashMap();
        settings.put(ConfigConstants.QUEUE_NAME, "dummyQueueName");
        httpSinkTask.start(settings);
    }
 @Test
    public void test_start_with_custom_trust_store_path_and_password() {
        Map<String, String> settings = Maps.newHashMap();
        String truststorePath = Thread.currentThread().getContextClassLoader().getResource(CLIENT_TRUSTSTORE_JKS_FILENAME).getPath();
        String password = CLIENT_TRUSTSTORE_JKS_PASSWORD;
        settings.put(HTTPCLIENT_SSL_TRUSTSTORE_PATH, truststorePath);
        settings.put(HTTPCLIENT_SSL_TRUSTSTORE_PASSWORD, password);
        httpSinkTask.start(settings);
    }
    @Test
    public void test_start_with_custom_trust_store_path_password_and_type() {
        Map<String, String> settings = Maps.newHashMap();
        String truststorePath = Thread.currentThread().getContextClassLoader().getResource(CLIENT_TRUSTSTORE_JKS_FILENAME).getPath();
        String password = CLIENT_TRUSTSTORE_JKS_PASSWORD;
        settings.put(HTTPCLIENT_SSL_TRUSTSTORE_PATH, truststorePath);
        settings.put(HTTPCLIENT_SSL_TRUSTSTORE_PASSWORD, password);
        settings.put(HTTPCLIENT_SSL_TRUSTSTORE_TYPE, JKS_STORE_TYPE);
        httpSinkTask.start(settings);
    }
    @Test
    public void test_start_with_custom_trust_store_path_password_type_and_algorithm() {
        Map<String, String> settings = Maps.newHashMap();
        String truststorePath = Thread.currentThread().getContextClassLoader().getResource(CLIENT_TRUSTSTORE_JKS_FILENAME).getPath();
        String password = CLIENT_TRUSTSTORE_JKS_PASSWORD;
        settings.put(HTTPCLIENT_SSL_TRUSTSTORE_PATH, truststorePath);
        settings.put(HTTPCLIENT_SSL_TRUSTSTORE_PASSWORD, password);
        settings.put(HTTPCLIENT_SSL_TRUSTSTORE_TYPE, JKS_STORE_TYPE);
        settings.put(HTTPCLIENT_SSL_TRUSTSTORE_ALGORITHM, TRUSTSTORE_PKIX_ALGORITHM);
        httpSinkTask.start(settings);
    }

    @Test
    public void test_start_with_static_request_headers() {
        Map<String, String> settings = Maps.newHashMap();
        settings.put(STATIC_REQUEST_HEADER_NAMES, "param1,param2");
        settings.put("param1", "value1");
        settings.put("param2", "value2");
        httpSinkTask.start(settings);
    }

    @Test
    public void test_start_with_static_request_headers_without_required_parameters() {
        Assertions.assertThrows(NullPointerException.class, () -> {
            HttpSinkTask wsSinkTask = new HttpSinkTask();
            Map<String, String> settings = Maps.newHashMap();
            settings.put(STATIC_REQUEST_HEADER_NAMES, "param1,param2");
            wsSinkTask.start(settings);
        });

    }


    @Test
    public void test_start_no_settings() {
        httpSinkTask.start(Maps.newHashMap());
    }


    @Test
    public void test_put_add_static_headers() {
        Map<String, String> settings = Maps.newHashMap();
        settings.put(STATIC_REQUEST_HEADER_NAMES, "param1,param2");
        settings.put("param1", "value1");
        settings.put("param2", "value2");
        httpSinkTask.start(settings);
        AHCHttpClient httpClient = mock(AHCHttpClient.class);
        HttpExchange dummyHttpExchange = getDummyHttpExchange();
        when(httpClient.call(any(HttpRequest.class),any(AtomicInteger.class))).thenReturn(dummyHttpExchange);
        httpSinkTask.setHttpClient(httpClient);
        List<SinkRecord> records = Lists.newArrayList();
        List<Header> headers = Lists.newArrayList();
        SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, getDummyHttpRequestAsString(), -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
        records.add(sinkRecord);
        httpSinkTask.put(records);
        ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(httpClient, times(1)).call(captor.capture(),any(AtomicInteger.class));
        HttpRequest enhancedRecordBeforeHttpCall = captor.getValue();
        assertThat(enhancedRecordBeforeHttpCall.getHeaders().size() == sinkRecord.headers().size() + httpSinkTask.getStaticRequestHeaders().size());
        assertThat(enhancedRecordBeforeHttpCall.getHeaders()).contains(Map.entry("param1", Lists.newArrayList("value1")));
        assertThat(enhancedRecordBeforeHttpCall.getHeaders()).contains(Map.entry("param2", Lists.newArrayList("value2")));
    }

    @Test
    public void test_put_nominal_case() {
        //given
        Map<String, String> settings = Maps.newHashMap();
        httpSinkTask.start(settings);

        //mock httpClient
        AHCHttpClient httpClient = mock(AHCHttpClient.class);
        HttpExchange dummyHttpExchange = getDummyHttpExchange();
        when(httpClient.call(any(HttpRequest.class),any(AtomicInteger.class))).thenReturn(dummyHttpExchange);
        httpSinkTask.setHttpClient(httpClient);

        //init sinkRecord
        List<SinkRecord> records = Lists.newArrayList();
        List<Header> headers = Lists.newArrayList();
        SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, getDummyHttpRequestAsString(), -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
        records.add(sinkRecord);

        //when
        httpSinkTask.put(records);

        //then

        //no additional headers added
        ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(httpClient, times(1)).call(captor.capture(),any(AtomicInteger.class));
        HttpRequest enhancedRecordBeforeHttpCall = captor.getValue();
        assertThat(enhancedRecordBeforeHttpCall.getHeaders().size() == sinkRecord.headers().size());

        //no records are published into the in memory queue by default
        verify(dummyQueue, never()).offer(any(HttpExchange.class));
    }

    @Test
    public void test_put_sink_record_with_null_value() {
        //given
        Map<String, String> settings = Maps.newHashMap();
        httpSinkTask.start(settings);

        //mock httpClient
        AHCHttpClient httpClient = mock(AHCHttpClient.class);
        HttpExchange dummyHttpExchange = getDummyHttpExchange();
        when(httpClient.call(any(HttpRequest.class),any(AtomicInteger.class))).thenReturn(dummyHttpExchange);
        httpSinkTask.setHttpClient(httpClient);

        //init sinkRecord
        List<SinkRecord> records = Lists.newArrayList();
        List<Header> headers = Lists.newArrayList();
        SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, null, -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
        records.add(sinkRecord);

        //when
        Assertions.assertThrows(ConnectException.class,()->httpSinkTask.put(records));

    }

    @Test
    public void test_put_with_publish_to_in_memory_queue_without_consumer() {
        //given
        Map<String, String> settings = Maps.newHashMap();
        settings.put(PUBLISH_TO_IN_MEMORY_QUEUE, "true");
        settings.put(ConfigConstants.QUEUE_NAME, "test");
        settings.put(WAIT_TIME_REGISTRATION_QUEUE_CONSUMER_IN_MS, "200");
        //when
        //then
        Assertions.assertThrows(IllegalArgumentException.class,
                () ->  httpSinkTask.start(settings));

    }


    @Test
    public void test_put_with_publish_in_memory_set_to_false() {
        Map<String, String> settings = Maps.newHashMap();
        settings.put(PUBLISH_TO_IN_MEMORY_QUEUE, "false");
        httpSinkTask.start(settings);
        AHCHttpClient httpClient = mock(AHCHttpClient.class);
        HttpExchange dummyHttpExchange = getDummyHttpExchange();
        when(httpClient.call(any(HttpRequest.class),any(AtomicInteger.class))).thenReturn(dummyHttpExchange);
        httpSinkTask.setHttpClient(httpClient);
        Queue<HttpExchange> queue = mock(Queue.class);
        httpSinkTask.setQueue(queue);
        List<SinkRecord> records = Lists.newArrayList();
        List<Header> headers = Lists.newArrayList();
        SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, getDummyHttpRequestAsString(), -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
        records.add(sinkRecord);
        httpSinkTask.put(records);
        verify(httpClient, times(1)).call(any(HttpRequest.class),any(AtomicInteger.class));
        verify(queue, never()).offer(any(HttpExchange.class));
    }

    @Test
    public void test_put_with_publish_to_in_memory_queue_set_to_true_with_a_consumer() {

        //given
        Map<String, String> settings = Maps.newHashMap();
        settings.put(PUBLISH_TO_IN_MEMORY_QUEUE, "true");
        QueueFactory.registerConsumerForQueue(QueueFactory.DEFAULT_QUEUE_NAME);
        httpSinkTask.start(settings);
        AHCHttpClient httpClient = mock(AHCHttpClient.class);
        HttpExchange dummyHttpExchange = getDummyHttpExchange();
        when(httpClient.call(any(HttpRequest.class),any(AtomicInteger.class))).thenReturn(dummyHttpExchange);
        httpSinkTask.setHttpClient(httpClient);
        Queue<HttpExchange> queue = mock(Queue.class);
        httpSinkTask.setQueue(queue);
        List<SinkRecord> records = Lists.newArrayList();
        List<Header> headers = Lists.newArrayList();
        SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, getDummyHttpRequestAsString(), -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
        records.add(sinkRecord);
        //when
        httpSinkTask.put(records);

        //then
        verify(httpClient, times(1)).call(any(HttpRequest.class),any(AtomicInteger.class));
        verify(queue, times(1)).offer(any(HttpExchange.class));
    }


    @Test
    public void test_http_exchange_json_serialization() throws JsonProcessingException, JSONException {
        HttpExchange dummyHttpExchange = getDummyHttpExchange();
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        String httpExchangeAsString = objectMapper.writeValueAsString(dummyHttpExchange);
        String expectedJSON = "" +
                "{\n" +
                "  \"durationInMillis\": 245,\n" +
                "  \"moment\": 1668388166.569457181,\n" +
                "  \"attempts\": 1,\n" +
                "  \"success\": true,\n" +
                "  \"httpResponse\": {\n" +
                "    \"statusCode\": 200,\n" +
                "    \"statusMessage\": \"OK\",\n" +
                "    \"responseBody\": \"my response\",\n" +
                "    \"responseHeaders\": {\n" +
                "      \"Content-Type\": [\"application/json\"]\n" +
                "    }\n" +
                "  },\n" +
                "  \"httpRequest\": {\n" +
                "    \"url\": \"http://www.titi.com\",\n" +
                "    \"headers\": {\n" +
                "      \"X-dummy\": [\n" +
                "        \"blabla\"\n" +
                "      ]\n" +
                "    },\n" +
                "    \"method\": \"" + DUMMY_METHOD + "\",\n" +
                "    \"bodyAsString\": \"" + DUMMY_BODY + "\",\n" +
                "    \"bodyAsByteArray\": \"\",\n" +
                "    \"bodyAsMultipart\": [],\n" +
                "    \"bodyType\": \"" + DUMMY_BODY_TYPE + "\"\n" +
                "  }\n" +
                "}";

        JSONAssert.assertEquals(expectedJSON, httpExchangeAsString,
                new CustomComparator(JSONCompareMode.LENIENT,
                        new Customization("moment", (o1, o2) -> true),
                        new Customization("durationInMillis", (o1, o2) -> true)
                ));


    }


    @Test
    public void test_buildHttpRequest_null_sink_record() {
        //when
        //then
        Assertions.assertThrows(ConnectException.class, () -> httpSinkTask.buildHttpRequest(null));
    }

    @Test
    public void test_buildHttpRequest_null_value_sink_record() {
        //when
        List<Header> headers = Lists.newArrayList();
        SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, null, -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
        //then
        Assertions.assertThrows(ConnectException.class, () -> httpSinkTask.buildHttpRequest(sinkRecord));
    }

    @Test
    public void test_buildHttpRequest_http_request_as_string() {
        //given
        List<Header> headers = Lists.newArrayList();
        SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, getDummyHttpRequestAsString(), -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
        //when
        HttpRequest httpRequest = httpSinkTask.buildHttpRequest(sinkRecord);
        //then
        assertThat(httpRequest).isNotNull();
        assertThat(httpRequest.getUrl()).isEqualTo(DUMMY_URL);
        assertThat(httpRequest.getMethod()).isEqualTo(DUMMY_METHOD);
        assertThat(httpRequest.getBodyType().toString()).isEqualTo(DUMMY_BODY_TYPE);
    }

    @Test
    public void test_buildHttpRequest_http_request_as_struct() {
        //given
        List<Header> headers = Lists.newArrayList();
        SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, getDummyHttpRequestAsStruct(), -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
        //when
        HttpRequest httpRequest = httpSinkTask.buildHttpRequest(sinkRecord);
        //then
        assertThat(httpRequest).isNotNull();
        assertThat(httpRequest.getUrl()).isEqualTo(DUMMY_URL);
        assertThat(httpRequest.getMethod()).isEqualTo(DUMMY_METHOD);
        assertThat(httpRequest.getBodyType().toString()).isEqualTo(DUMMY_BODY_TYPE);
    }




    @Test
    public void test_retry_needed(){
        HttpResponse httpResponse = new HttpResponse(500,"Internal Server Error","");
        Map<String, String> settings = Maps.newHashMap();
        httpSinkTask.start(settings);
        boolean retryNeeded = httpSinkTask.retryNeeded(httpResponse);
        assertThat(retryNeeded).isTrue();
    }
    @Test
    public void test_retry_not_needed_with_400_status_code(){
        HttpResponse httpResponse = new HttpResponse(400,"Internal Server Error","");
        Map<String, String> settings = Maps.newHashMap();
        httpSinkTask.start(settings);
        boolean retryNeeded = httpSinkTask.retryNeeded(httpResponse);
        assertThat(retryNeeded).isFalse();
    }
    @Test
    public void test_retry_not_needed_with_200_status_code(){
        HttpResponse httpResponse = new HttpResponse(200,"Internal Server Error","");
        Map<String, String> settings = Maps.newHashMap();
        httpSinkTask.start(settings);
        boolean retryNeeded = httpSinkTask.retryNeeded(httpResponse);
        assertThat(retryNeeded).isFalse();
    }


    @Test
    public void test_retry_needed_by_configuration_with_200_status_code(){
        HttpResponse httpResponse = new HttpResponse(200,"Internal Server Error","");
        Map<String, String> settings = Maps.newHashMap();
        settings.put(DEFAULT_RETRY_RESPONSE_CODE_REGEX,"^[1-5][0-9][0-9]$");
        httpSinkTask.start(settings);
        boolean retryNeeded = httpSinkTask.retryNeeded(httpResponse);
        assertThat(retryNeeded).isTrue();
    }
  @Test
    public void test_is_success_with_200(){
        HttpExchange httpExchange = getDummyHttpExchange();
        Map<String, String> settings = Maps.newHashMap();
        httpSinkTask.start(settings);
        boolean success = httpSinkTask.isSuccess(httpExchange);
        assertThat(success).isTrue();
    }

    @Test
    public void test_is_not_success_with_200_by_configuration(){
        HttpExchange httpExchange = getDummyHttpExchange();
        Map<String, String> settings = Maps.newHashMap();
        settings.put(DEFAULT_SUCCESS_RESPONSE_CODE_REGEX,"^20[1-5]$");
        httpSinkTask.start(settings);
        boolean success = httpSinkTask.isSuccess(httpExchange);
        assertThat(success).isFalse();
    }


    private HttpExchange getDummyHttpExchange() {
        Map<String, List<String>> requestHeaders = Maps.newHashMap();
        requestHeaders.put("X-dummy", Lists.newArrayList("blabla"));
        HttpRequest httpRequest = new HttpRequest("http://www.titi.com", DUMMY_METHOD, DUMMY_BODY_TYPE, DUMMY_BODY, null, null);
        httpRequest.setHeaders(requestHeaders);
        HttpResponse httpResponse = new HttpResponse(200, "OK", "my response");
        Map<String, List<String>> responseHeaders = Maps.newHashMap();
        responseHeaders.put("Content-Type", Lists.newArrayList("application/json"));
        httpResponse.setResponseHeaders(responseHeaders);
        return new HttpExchange(
                httpRequest,
                httpResponse,
                245L,
                OffsetDateTime.now(ZoneId.of("UTC")),
                new AtomicInteger(1),
                true
        );
    }


    private String getDummyHttpRequestAsString() {
        return "{\n" +
                "  \"url\": \"" + DUMMY_URL + "\",\n" +
                "  \"headers\": {},\n" +
                "  \"method\": \"" + DUMMY_METHOD + "\",\n" +
                "  \"bodyAsString\": \"" + DUMMY_BODY + "\",\n" +
                "  \"bodyAsByteArray\": null,\n" +
                "  \"bodyAsMultipart\": null,\n" +
                "  \"bodyType\": \"" + DUMMY_BODY_TYPE + "\"\n" +
                "}";
    }

    private Struct getDummyHttpRequestAsStruct() {
        HttpRequest httpRequest = new HttpRequest(DUMMY_URL,DUMMY_METHOD,DUMMY_BODY_TYPE,DUMMY_BODY,null,null);
        return httpRequest.toStruct();
    }

}