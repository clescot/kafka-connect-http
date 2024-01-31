package io.github.clescot.kafka.connect.http;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.trafficlistener.ConsoleNotifyingWiremockNetworkTrafficListener;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.github.clescot.kafka.connect.http.client.Configuration;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpRequestAsStruct;
import io.github.clescot.kafka.connect.http.sink.HttpSinkConnectorConfig;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.jmx.JmxMeterRegistry;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.assertj.core.util.Sets;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE;
import static io.github.clescot.kafka.connect.http.core.HttpRequestAsStruct.SCHEMA;
import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;
import static org.assertj.core.api.Assertions.assertThat;

class HttpTaskTest {
    private static final String DUMMY_BODY = "stuff";
    private static final String DUMMY_URL = "http://www." + DUMMY_BODY + ".com";
    private static final String DUMMY_METHOD = "POST";
    private static final String DUMMY_BODY_TYPE = "STRING";
    private static final ExecutorService executorService = Executors.newFixedThreadPool(2);
    public static final String AUTHORIZED_STATE = "Authorized";
    public static final String INTERNAL_SERVER_ERROR_STATE = "InternalServerError";
    @RegisterExtension
    static WireMockExtension wmHttp;

    static {

        wmHttp = WireMockExtension.newInstance()
                .options(
                        WireMockConfiguration.wireMockConfig()
                                .dynamicPort()
                                .networkTrafficListener(new ConsoleNotifyingWiremockNetworkTrafficListener())
                                .useChunkedTransferEncoding(Options.ChunkedEncodingPolicy.NEVER)
                )
                .build();
    }

    @Nested
    class BuildHttpRequest {

        private HttpTask<SinkRecord> httpTask;

        @BeforeEach
        public void setUp(){
            Map<String,String> configs = Maps.newHashMap();
            configs.put(HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE,"2");
            AbstractConfig config = new HttpSinkConnectorConfig(configs);
            httpTask = new HttpTask<>(config);
        }
        @Test
        void test_buildHttpRequest_null_sink_record() {
            //when
            //then
            Assertions.assertThrows(ConnectException.class, () -> httpTask.buildHttpRequest(null));
        }

        @Test
        void test_buildHttpRequest_null_value_sink_record() {
            //when
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, null, -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            //then
            Assertions.assertThrows(ConnectException.class, () -> httpTask.buildHttpRequest(sinkRecord));
        }

        @Test
        void test_buildHttpRequest_http_request_as_string() {
            //given
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, getDummyHttpRequestAsString(), -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            //when
            HttpRequest httpRequest = httpTask.buildHttpRequest(sinkRecord);
            //then
            assertThat(httpRequest).isNotNull();
            assertThat(httpRequest.getUrl()).isEqualTo(DUMMY_URL);
            assertThat(httpRequest.getMethod()).isEqualTo(DUMMY_METHOD);
            assertThat(httpRequest.getBodyType().toString()).hasToString(DUMMY_BODY_TYPE);
        }

        @Test
        void test_buildHttpRequest_http_request_as_json_schema() throws IOException {
            //given
            List<Header> headers = Lists.newArrayList();
            HttpRequest dummyHttpRequest = getDummyHttpRequest(DUMMY_URL);
            String topic = "myTopic";
            SchemaRegistryClient schemaRegistryClient = getSchemaRegistryClient();
            registerSchema(schemaRegistryClient, topic, 1, 1, HttpRequest.SCHEMA_AS_STRING);


            JsonSchemaConverter jsonSchemaConverter = getJsonSchemaConverter(schemaRegistryClient);


            byte[] httpRequestAsJsonSchemaWithConverter = jsonSchemaConverter.fromConnectData(topic, SCHEMA, new HttpRequestAsStruct(dummyHttpRequest).toStruct());

            SchemaAndValue schemaAndValue = jsonSchemaConverter.toConnectData(topic, httpRequestAsJsonSchemaWithConverter);

            SinkRecord sinkRecord = new SinkRecord(topic, 0, Schema.STRING_SCHEMA, "key", schemaAndValue.schema(), schemaAndValue.value(), -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            //when
            HttpRequest httpRequest = httpTask.buildHttpRequest(sinkRecord);
            //then
            assertThat(httpRequest).isNotNull();
            assertThat(httpRequest.getUrl()).isEqualTo(DUMMY_URL);
            assertThat(httpRequest.getMethod()).isEqualTo(DUMMY_METHOD);
            assertThat(httpRequest.getBodyType().toString()).hasToString(DUMMY_BODY_TYPE);
        }


        @Test
        void test_buildHttpRequest_http_request_as_struct() {
            //given
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, getDummyHttpRequestAsStruct(DUMMY_URL), -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            //when
            HttpRequest httpRequest = httpTask.buildHttpRequest(sinkRecord);
            //then
            assertThat(httpRequest).isNotNull();
            assertThat(httpRequest.getUrl()).isEqualTo(DUMMY_URL);
            assertThat(httpRequest.getMethod()).isEqualTo(DUMMY_METHOD);
            assertThat(httpRequest.getBodyType().toString()).hasToString(DUMMY_BODY_TYPE);
        }


    }


    @Nested
    class callWithRetryPolicy {
        private HttpTask<SinkRecord> httpTask;

        @BeforeEach
        public void setUp(){
            Map<String,String> configs = Maps.newHashMap();
            AbstractConfig config = new HttpSinkConnectorConfig(configs);
            httpTask = new HttpTask<>(config);
        }

        @Test
        void test_successful_request_at_first_time() throws ExecutionException, InterruptedException {

            //given
            String scenario = "test_successful_request_at_first_time";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs(STARTED)
                            .willReturn(WireMock.aResponse()
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                                    .withBody("")
                            ).willSetStateTo(AUTHORIZED_STATE)
                    );
            //when
            HttpRequest httpRequest = getDummyHttpRequest(wmHttp.url("/ping"));
            Map<String, String> settings = Maps.newHashMap();
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration<Request, Response> configuration = new Configuration<>("dummy",new OkHttpClientFactory(), httpSinkConnectorConfig, executorService, getCompositeMeterRegistry());
            HttpExchange httpExchange = httpTask.callWithRetryPolicy(httpRequest,configuration).get();

            //then
            assertThat(httpExchange.isSuccess()).isTrue();
        }
        @Test
        void test_successful_request_at_second_time() throws ExecutionException, InterruptedException {

            //given
            String scenario = "test_successful_request_at_second_time";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs(STARTED)
                            .willReturn(WireMock.aResponse()
                                    .withStatus(500)
                                    .withStatusMessage("Internal Server Error")
                            ).willSetStateTo(INTERNAL_SERVER_ERROR_STATE)
                    );
            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs(INTERNAL_SERVER_ERROR_STATE)
                            .willReturn(WireMock.aResponse()
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(AUTHORIZED_STATE)
                    );
            //when
            HttpRequest httpRequest = getDummyHttpRequest(wmHttp.url("/ping"));
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.dummy.retry.policy.retries","2");
            settings.put("config.dummy.retry.policy.response.code.regex",DEFAULT_DEFAULT_RETRY_RESPONSE_CODE_REGEX);
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration<Request, Response> configuration = new Configuration<>("dummy",new OkHttpClientFactory(), httpSinkConnectorConfig, executorService, getCompositeMeterRegistry());
            HttpExchange httpExchange = httpTask.callWithRetryPolicy(httpRequest,configuration).get();

            //then
            AtomicInteger attempts = httpExchange.getAttempts();
            assertThat(attempts.get()).isEqualTo(2);
            assertThat(httpExchange.isSuccess()).isTrue();
        }

    }

    @NotNull
    private static HttpRequest getDummyHttpRequest(String url) {
        HttpRequest httpRequest = new HttpRequest(url, DUMMY_METHOD, DUMMY_BODY_TYPE);
        Map<String, List<String>> headers = Maps.newHashMap();
        headers.put("Content-Type", Lists.newArrayList("application/json"));
        httpRequest.setHeaders(headers);
        httpRequest.setBodyAsString("stuff");
        httpRequest.setBodyAsForm(Maps.newHashMap());
        return httpRequest;
    }


    @NotNull
    private static JsonSchemaConverter getJsonSchemaConverter(SchemaRegistryClient mockSchemaRegistryClient) {
        JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter(mockSchemaRegistryClient);
        Map<String, String> config = Maps.newHashMap();
        config.put("schema.registry.url", "http://dummy.com");
        config.put(JSON_VALUE_TYPE, HttpRequest.class.getName());
        jsonSchemaConverter.configure(config, false);
        return jsonSchemaConverter;
    }

    private Struct getDummyHttpRequestAsStruct(String url) {
        HttpRequest httpRequest = getDummyHttpRequest(url);
        HttpRequestAsStruct httpRequestAsStruct = new HttpRequestAsStruct(httpRequest);
        return httpRequestAsStruct.toStruct();
    }


    private static void registerSchema(SchemaRegistryClient mockSchemaRegistryClient, String topic, int schemaVersion, int schemaId, String schemaAsString) {
        //we test TopicNameStrategy, and the jsonSchema is owned in the Kafka value record.
        String subject = topic + "-value";
        io.confluent.kafka.schemaregistry.client.rest.entities.Schema schema = new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(subject, schemaVersion, schemaId, JsonSchema.TYPE, Lists.newArrayList(), schemaAsString);
        Optional<ParsedSchema> parsedSchema = mockSchemaRegistryClient.parseSchema(schema);
        try {
            mockSchemaRegistryClient.register(subject, parsedSchema.get());
        } catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    private static SchemaRegistryClient getSchemaRegistryClient() {
        SchemaProvider provider = new JsonSchemaProvider();
        SchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient(Collections.singletonList(provider));
        return mockSchemaRegistryClient;
    }

    private static CompositeMeterRegistry getCompositeMeterRegistry() {
        JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry(s -> null, Clock.SYSTEM);
        HashSet<MeterRegistry> registries = Sets.newHashSet();
        registries.add(jmxMeterRegistry);
        return new CompositeMeterRegistry(Clock.SYSTEM, registries);
    }

    private String getDummyHttpRequestAsString() {
        return "{\n" +
                "  \"url\": \"" + DUMMY_URL + "\",\n" +
                "  \"headers\": {},\n" +
                "  \"method\": \"" + DUMMY_METHOD + "\",\n" +
                "  \"bodyAsString\": \"" + DUMMY_BODY + "\",\n" +
                "  \"bodyAsByteArray\": [],\n" +
                "  \"bodyAsForm\": {},\n" +
                "  \"bodyAsMultipart\": [],\n" +
                "  \"bodyType\": \"" + DUMMY_BODY_TYPE + "\"\n" +
                "}";
    }

    private String getIP() {
        try (DatagramSocket datagramSocket = new DatagramSocket()) {
            datagramSocket.connect(InetAddress.getByName("8.8.8.8"), 12345);
            return datagramSocket.getLocalAddress().getHostAddress();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}