package io.github.clescot.kafka.connect.http.sink;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.github.clescot.kafka.connect.MapUtils;
import io.github.clescot.kafka.connect.http.HttpTask;
import io.github.clescot.kafka.connect.http.client.HttpClientConfiguration;
import io.github.clescot.kafka.connect.http.client.HttpConfiguration;
import io.github.clescot.kafka.connect.http.client.ahc.AHCHttpClient;
import io.github.clescot.kafka.connect.http.client.config.AddSuccessStatusToHttpExchangeFunction;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.client.ssl.AlwaysTrustManagerFactory;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.core.queue.ConfigConstants;
import io.github.clescot.kafka.connect.http.core.queue.KafkaRecord;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.github.clescot.kafka.connect.http.mapper.DirectHttpRequestMapper;
import io.github.clescot.kafka.connect.http.mapper.JEXLHttpRequestMapper;
import io.github.clescot.kafka.connect.http.mapper.MapperMode;
import io.github.clescot.kafka.connect.http.sink.publish.PublishMode;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition.*;
import static io.github.clescot.kafka.connect.http.client.HttpClientFactory.defaultSuccessPattern;
import static io.github.clescot.kafka.connect.http.client.config.HttpRequestPredicateBuilder.URL_REGEX;
import static io.github.clescot.kafka.connect.http.mapper.HttpRequestMapperFactory.JEXL_ALWAYS_MATCHES;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.*;
import static io.github.clescot.kafka.connect.http.sink.SinkConfigDefinition.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;

@Execution(ExecutionMode.SAME_THREAD)
public class HttpSinkTaskTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSinkTaskTest.class);
    private static final String DUMMY_BODY = "stuff";
    private static final String DUMMY_URL = "http://www." + DUMMY_BODY + ".com";
    private static final HttpRequest.Method DUMMY_METHOD = HttpRequest.Method.POST;
    private static final String DUMMY_BODY_TYPE = "STRING";
    public static final String CLIENT_TRUSTSTORE_JKS_FILENAME = "client_truststore.jks";
    public static final String CLIENT_TRUSTSTORE_JKS_PASSWORD = "Secret123!";
    public static final String JKS_STORE_TYPE = "jks";
    public static final String TRUSTSTORE_PKIX_ALGORITHM = "PKIX";
    public static final String BEARER_TOKEN = "Bearer BQDzs98uhifaGayk8H9tCTRozufhFmgV_HKMCnnDdMTdz1FcOo3sdj8OZJ_azo96LRdLI9_1uJOCXxbGZme11KCb6ZxTuCt8B5FxEeECb1kO_-UDuf8";
    public static final String BEARER_TOKEN_2 = "Bearer BQDzs98uhifaGayk8H9tCTRozufhFmgV_AAAAAAAAdz1FcOo3sdj8OZJ_BBBBBBBBBBB_1uJOCXxbGZme11KCb6ZxTuCt8B5FxEeECb1kO_-UDuf8";
    public static final String OK = "OK";
    public static final String AUTHORIZED_STATE = "Authorized";
    private static final ExecutorService executorService = Executors.newFixedThreadPool(2);

    @Mock
    ErrantRecordReporter errantRecordReporter;
    @Mock
    SinkTaskContext sinkTaskContext;

    @Mock
    Queue<HttpExchange> dummyQueue;



    @InjectMocks
    OkHttpSinkTask okHttpSinkTask;

    @InjectMocks
    AHCSinkTask ahcSinkTask;


    @RegisterExtension
    static WireMockExtension wmHttp = WireMockExtension.newInstance()
            .options(
                    WireMockConfiguration.wireMockConfig()
                            .dynamicPort()
            )
            .build();


    @BeforeEach
    void setUp() {
        QueueFactory.clearRegistrations();
        MockitoAnnotations.openMocks(this);
        okHttpSinkTask.initialize(sinkTaskContext);
        ahcSinkTask.initialize(sinkTaskContext);
    }

    @AfterEach
    void tearsDown() {
        wmHttp.resetAll();
        HttpTask.removeCompositeMeterRegistry();
    }



    @Nested
    class Start {

        @Test
        void test_start_with_queue_name() {
            Assertions.assertDoesNotThrow(() -> {
                Map<String, String> settings = Maps.newHashMap();
                settings.put(ConfigConstants.QUEUE_NAME, "dummyQueueName");
                okHttpSinkTask.start(settings);
            });
        }


        @Test
        void test_start_with_static_request_headers() {
            Assertions.assertDoesNotThrow(() -> {
                Map<String, String> settings = Maps.newHashMap();
                settings.put(STATIC_REQUEST_HEADER_NAMES, "param1,param2");
                settings.put("param1", "value1");
                settings.put("param2", "value2");
                okHttpSinkTask.start(settings);
            });
        }

        @Test
        void test_start_with_static_request_headers_without_required_parameters() {
            OkHttpSinkTask wsSinkTask = new OkHttpSinkTask();
            Map<String, String> settings = Maps.newHashMap();
            settings.put(STATIC_REQUEST_HEADER_NAMES, "param1,param2");
            Assertions.assertThrows(NullPointerException.class, () -> {
                wsSinkTask.start(settings);
            });

        }


        @Test
        void test_start_no_settings() {
            Assertions.assertDoesNotThrow(() -> {
                okHttpSinkTask.start(Maps.newHashMap());
            });
        }


    }

    @Nested
    class StartWithHttpRequestMapper {
        @Test
        void test_start_without_settings_default_httprequestmapper_is_direct() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                okHttpSinkTask.start(settings);
                assertThat(okHttpSinkTask.getDefaultHttpRequestMapper()).isInstanceOf(DirectHttpRequestMapper.class);
            });
        }

        @Test
        void test_start_with_settings_httprequestmapper_is_direct() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                settings.put(REQUEST_MAPPER_DEFAULT_MODE, MapperMode.DIRECT.name());
                okHttpSinkTask.start(settings);
                assertThat(okHttpSinkTask.getDefaultHttpRequestMapper()).isInstanceOf(DirectHttpRequestMapper.class);
                DirectHttpRequestMapper httpRequestMapper = (DirectHttpRequestMapper) okHttpSinkTask.getDefaultHttpRequestMapper();
                assertThat(httpRequestMapper.getExpression().getSourceText()).isEqualTo(JEXL_ALWAYS_MATCHES);
            });
        }

        @Test
        void test_start_with_settings_httprequestmapper_is_jexl() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                settings.put(DEFAULT_REQUEST_MAPPER_PREFIX + REQUEST_MAPPER_DEFAULT_MODE, MapperMode.JEXL.name());
                settings.put(DEFAULT_REQUEST_MAPPER_PREFIX + REQUEST_MAPPER_DEFAULT_URL_EXPRESSION, "sinkRecord.value()");
                settings.put(DEFAULT_REQUEST_MAPPER_PREFIX + REQUEST_MAPPER_DEFAULT_METHOD_EXPRESSION, "'GET'");
                okHttpSinkTask.start(settings);
                assertThat(okHttpSinkTask.getDefaultHttpRequestMapper()).isInstanceOf(JEXLHttpRequestMapper.class);
                JEXLHttpRequestMapper httpRequestMapper = (JEXLHttpRequestMapper) okHttpSinkTask.getDefaultHttpRequestMapper();
                assertThat(httpRequestMapper.getJexlMatchingExpression().getSourceText()).isEqualTo("true");
                assertThat(httpRequestMapper.getJexlUrlExpression().getSourceText()).isEqualTo("sinkRecord.value()");
                assertThat(httpRequestMapper.getJexlMethodExpression().get().getSourceText()).isEqualTo("'GET'");
                assertThat(httpRequestMapper.getJexlBodyExpression()).isEmpty();
                assertThat(httpRequestMapper.getJexlBodyTypeExpression().get().getSourceText()).isEqualTo("'STRING'");
                assertThat(httpRequestMapper.getJexlBodyExpression()).isEmpty();
                assertThat(httpRequestMapper.getJexlHeadersExpression()).isEmpty();
            });
        }

        @Test
        void test_start_with_more_settings_httprequestmapper_is_jexl() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                settings.put(DEFAULT_REQUEST_MAPPER_PREFIX + REQUEST_MAPPER_DEFAULT_MODE, MapperMode.JEXL.name());
                settings.put(DEFAULT_REQUEST_MAPPER_PREFIX + REQUEST_MAPPER_DEFAULT_URL_EXPRESSION, "sinkRecord.value()");
                settings.put(DEFAULT_REQUEST_MAPPER_PREFIX + REQUEST_MAPPER_DEFAULT_METHOD_EXPRESSION, "'GET'");
                settings.put(DEFAULT_REQUEST_MAPPER_PREFIX + REQUEST_MAPPER_DEFAULT_BODY_EXPRESSION, "'my request body'");
                settings.put(DEFAULT_REQUEST_MAPPER_PREFIX + REQUEST_MAPPER_DEFAULT_BODYTYPE_EXPRESSION, "'STRING'");
                settings.put(DEFAULT_REQUEST_MAPPER_PREFIX + REQUEST_MAPPER_DEFAULT_HEADERS_EXPRESSION, "{'test1':['value1','value2',...]}");
                okHttpSinkTask.start(settings);
                assertThat(okHttpSinkTask.getDefaultHttpRequestMapper()).isInstanceOf(JEXLHttpRequestMapper.class);
                JEXLHttpRequestMapper httpRequestMapper = (JEXLHttpRequestMapper) okHttpSinkTask.getDefaultHttpRequestMapper();
                assertThat(httpRequestMapper.getJexlMatchingExpression().getSourceText()).isEqualTo("true");
                assertThat(httpRequestMapper.getJexlUrlExpression().getSourceText()).isEqualTo("sinkRecord.value()");
                assertThat(httpRequestMapper.getJexlMethodExpression().get().getSourceText()).isEqualTo("'GET'");
                assertThat(httpRequestMapper.getJexlBodyExpression().get().getSourceText()).isEqualTo("'my request body'");
                assertThat(httpRequestMapper.getJexlBodyTypeExpression().get().getSourceText()).isEqualTo("'STRING'");
                assertThat(httpRequestMapper.getJexlHeadersExpression().get().getSourceText()).isEqualTo("{'test1':['value1','value2',...]}");
            });
        }
    }

    @Nested
    class StartWithCustomConfigurations {
        @Test
        void test_start_with_one_custom_configuration() {
            Assertions.assertDoesNotThrow(() -> {

                Map<String, String> settings = Maps.newHashMap();
                settings.put("config.ids","config1");
                settings.put("config1."+URL_REGEX,"http://toto\\.com");
                okHttpSinkTask.start(settings);
                OkHttpClient httpClient = Mockito.mock(OkHttpClient.class);
                when(httpClient.call(any(HttpRequest.class), any(AtomicInteger.class))).thenReturn(CompletableFuture.supplyAsync(() -> getHttpExchange("https://toto.com",HttpRequest.Method.GET,200)));
                when(httpClient.getEnrichRequestFunction()).thenReturn(request->request);
                okHttpSinkTask.getDefaultConfiguration().getConfiguration().setHttpClient(httpClient);
                Collection<SinkRecord> records = Lists.newArrayList();
                SinkRecord myRecord = new SinkRecord("myTopic",0, Schema.STRING_SCHEMA,"key",Schema.STRING_SCHEMA,getHttpRequestAsString("https://toto.com",HttpRequest.Method.GET),0L);
                records.add(myRecord);
                okHttpSinkTask.put(records);
                verify(httpClient, times(1)).call(any(HttpRequest.class), any(AtomicInteger.class));
            });
        }
    }


    @Nested
    class StartWithSsl {
        @Test
        void test_start_with_custom_trust_store_path_and_password() {
            Assertions.assertDoesNotThrow(() -> {
                Map<String, String> settings = Maps.newHashMap();
                URL resource = Thread.currentThread().getContextClassLoader().getResource(CLIENT_TRUSTSTORE_JKS_FILENAME);
                Preconditions.checkNotNull(resource);
                String truststorePath = resource.getPath();
                settings.put(HTTP_CLIENT_SSL_TRUSTSTORE_PATH, truststorePath);
                settings.put(HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD, CLIENT_TRUSTSTORE_JKS_PASSWORD);
                okHttpSinkTask.start(settings);
            });
        }

        @Test
        void test_start_with_custom_trust_store_path_password_and_type() {
            Assertions.assertDoesNotThrow(() -> {
                Map<String, String> settings = Maps.newHashMap();
                URL resource = Thread.currentThread().getContextClassLoader().getResource(CLIENT_TRUSTSTORE_JKS_FILENAME);
                Preconditions.checkNotNull(resource);
                String truststorePath = resource.getPath();
                settings.put(HTTP_CLIENT_SSL_TRUSTSTORE_PATH, truststorePath);
                settings.put(HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD, CLIENT_TRUSTSTORE_JKS_PASSWORD);
                settings.put(HTTP_CLIENT_SSL_TRUSTSTORE_TYPE, JKS_STORE_TYPE);
                okHttpSinkTask.start(settings);
            });

        }

        @Test
        void test_start_with_custom_trust_store_path_password_type_and_algorithm() {
            Assertions.assertDoesNotThrow(() -> {
                Map<String, String> settings = Maps.newHashMap();
                URL resource = Thread.currentThread().getContextClassLoader().getResource(CLIENT_TRUSTSTORE_JKS_FILENAME);
                Preconditions.checkNotNull(resource);
                String truststorePath = resource.getPath();
                settings.put(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PATH, truststorePath);
                settings.put(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD, CLIENT_TRUSTSTORE_JKS_PASSWORD);
                settings.put(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_TYPE, JKS_STORE_TYPE);
                settings.put(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM, TRUSTSTORE_PKIX_ALGORITHM);
                okHttpSinkTask.start(settings);
            });
        }

        @Test
        void test_ssl_always_granted_parameter() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST, "true");
            okHttpSinkTask.start(settings);
            TrustManagerFactory trustManagerFactory = okHttpSinkTask.getHttpTask().getDefaultConfiguration().getClient().getTrustManagerFactory();
            assertThat(trustManagerFactory).isInstanceOf(AlwaysTrustManagerFactory.class);
        }

    }

    @Nested
    class Stop {
        @Test
        void test_stop_with_start_and_no_setttings() {
            okHttpSinkTask.start(Maps.newHashMap());
            Assertions.assertDoesNotThrow(() -> okHttpSinkTask.stop());
        }

        @Test
        void test_stop_without_start() {
            Assertions.assertDoesNotThrow(() -> okHttpSinkTask.stop());
        }

    }

    @Nested
    class Put {

        @Test
        void test_put_with_no_records() {
            //given
            Map<String, String> settings = Maps.newHashMap();
            ahcSinkTask.start(settings);

            //mock httpClient
            AHCHttpClient httpClient = Mockito.mock(AHCHttpClient.class);
            HttpExchange dummyHttpExchange = getHttpExchange();
            when(httpClient.call(any(HttpRequest.class), any(AtomicInteger.class))).thenReturn(CompletableFuture.supplyAsync(() -> dummyHttpExchange));
            ahcSinkTask.getDefaultConfiguration().getConfiguration().setHttpClient(httpClient);

            //init sinkRecord
            List<SinkRecord> records = Lists.newArrayList();

            //when
            ahcSinkTask.put(records);

            //then

            //no additional headers added
            ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
            verify(httpClient,never()).call(captor.capture(), any(AtomicInteger.class));

            //no records are published into the in memory queue by default
            verify(dummyQueue, never()).offer(any(HttpExchange.class));
        }




        @Test
        void test_put_nominal_case_with_value_as_string() {
            //given
            Map<String, String> settings = Maps.newHashMap();
            ahcSinkTask.start(settings);

            //mock httpClient
            AHCHttpClient httpClient = Mockito.mock(AHCHttpClient.class);
            HttpExchange dummyHttpExchange = getHttpExchange();
            when(httpClient.call(any(HttpRequest.class), any(AtomicInteger.class))).thenReturn(CompletableFuture.supplyAsync(() -> dummyHttpExchange));
            when(httpClient.getEnrichRequestFunction()).thenReturn(request->request);
            ahcSinkTask.getDefaultConfiguration().getConfiguration().setHttpClient(httpClient);

            //init sinkRecord
            List<SinkRecord> records = Lists.newArrayList();
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, getDummyHttpRequestAsString(), -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord);

            //when
            ahcSinkTask.put(records);

            //then

            //no additional headers added
            ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
            verify(httpClient, times(1)).call(captor.capture(), any(AtomicInteger.class));
            HttpRequest enhancedRecordBeforeHttpCall = captor.getValue();
            assertThat(enhancedRecordBeforeHttpCall.getHeaders()).hasSameSizeAs(sinkRecord.headers());

            //no records are published into the in memory queue by default
            verify(dummyQueue, never()).offer(any(HttpExchange.class));
        }

        @Test
        void test_put_nominal_case_with_value_as_json_schema() {
            //given
            Map<String, String> settings = Maps.newHashMap();
            ahcSinkTask.start(settings);

            //mock httpClient
            AHCHttpClient httpClient = Mockito.mock(AHCHttpClient.class);
            HttpExchange dummyHttpExchange = getHttpExchange();
            when(httpClient.call(any(HttpRequest.class), any(AtomicInteger.class))).thenReturn(CompletableFuture.supplyAsync(() -> dummyHttpExchange));
            when(httpClient.getEnrichRequestFunction()).thenReturn(request->request);
            ahcSinkTask.getDefaultConfiguration().getConfiguration().setHttpClient(httpClient);

            //init sinkRecord
            List<SinkRecord> records = Lists.newArrayList();
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, getDummyHttpRequestAsString(), -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord);

            //when
            ahcSinkTask.put(records);

            //then

            //no additional headers added
            ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
            verify(httpClient, times(1)).call(captor.capture(), any(AtomicInteger.class));
            HttpRequest enhancedRecordBeforeHttpCall = captor.getValue();
            assertThat(enhancedRecordBeforeHttpCall.getHeaders()).hasSameSizeAs(sinkRecord.headers());

            //no records are published into the in memory queue by default
            verify(dummyQueue, never()).offer(any(HttpExchange.class));
        }

        @Test
        void test_put_sink_record_with_null_value() {
            //given
            Map<String, String> settings = Maps.newHashMap();
            ahcSinkTask.start(settings);

            //mock httpClient
            AHCHttpClient httpClient = Mockito.mock(AHCHttpClient.class);
            HttpExchange dummyHttpExchange = getHttpExchange();
            when(httpClient.call(any(HttpRequest.class), any(AtomicInteger.class))).thenReturn(CompletableFuture.supplyAsync(() -> dummyHttpExchange));
            ahcSinkTask.getDefaultConfiguration().getConfiguration().setHttpClient(httpClient);

            //init sinkRecord
            List<SinkRecord> records = Lists.newArrayList();
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, null, -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord);

            //when
            Assertions.assertDoesNotThrow(() -> ahcSinkTask.put(records));

        }

        @Test
        void test_put_with_publish_to_in_memory_queue_without_consumer() {
            //given
            Map<String, String> settings = Maps.newHashMap();
            settings.put(PUBLISH_MODE, PublishMode.IN_MEMORY_QUEUE.name());
            settings.put(ConfigConstants.QUEUE_NAME, "test");
            settings.put(WAIT_TIME_REGISTRATION_QUEUE_CONSUMER_IN_MS, "200");
            //when
            //then
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> okHttpSinkTask.start(settings));

        }


        @Test
        void test_put_with_publish_mode_set_to_none() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put(PUBLISH_MODE, PublishMode.NONE.name());
            ahcSinkTask.start(settings);
            AHCHttpClient httpClient = Mockito.mock(AHCHttpClient.class);
            HttpExchange dummyHttpExchange = getHttpExchange();
            when(httpClient.call(any(HttpRequest.class), any(AtomicInteger.class))).thenReturn(CompletableFuture.supplyAsync(() -> dummyHttpExchange));
            when(httpClient.getEnrichRequestFunction()).thenReturn(request->request);
            ahcSinkTask.getDefaultConfiguration().getConfiguration().setHttpClient(httpClient);
            Queue<KafkaRecord> queue = mock(Queue.class);
            ahcSinkTask.setQueue(queue);
            List<SinkRecord> records = Lists.newArrayList();
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, getDummyHttpRequestAsString(), -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord);
            ahcSinkTask.put(records);
            verify(httpClient, times(1)).call(any(HttpRequest.class), any(AtomicInteger.class));
            verify(queue, never()).offer(any(KafkaRecord.class));
        }

        @Test
        void test_put_with_publish_mode_set_to_producer() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put(PUBLISH_MODE, PublishMode.PRODUCER.name());
            settings.put(PRODUCER_BOOTSTRAP_SERVERS, "127.0.0.1:9092");
            settings.put(PRODUCER_SUCCESS_TOPIC, "myTopic");
            Node node = new Node(0,"127.0.0.1",9092);
            Collection<Node> nodes = Lists.newArrayList(node);
            Collection<PartitionInfo> partitionInfos = Lists.newArrayList();
            Node[] nodesArray = new Node[]{node};
            PartitionInfo partitionInfo = new PartitionInfo("myTopic",0,node,nodesArray,nodesArray);
            partitionInfos.add(partitionInfo);

            Cluster cluster  = new Cluster("dummyClusterId",nodes,partitionInfos, Sets.newHashSet(),Sets.newHashSet());
            KafkaJsonSerializer<Object> jsonSerializer = new KafkaJsonSerializer<>();
            Map<String,Object> jsonSerializerConfig = Maps.newHashMap();
            jsonSerializer.configure(jsonSerializerConfig,false);
            MockProducer<String, Object> mockProducer = new MockProducer<>(cluster,true,new RoundRobinPartitioner(), new StringSerializer(), jsonSerializer);

            OkHttpSinkTask myOkHttpSinkTask = new OkHttpSinkTask(mockProducer);
            myOkHttpSinkTask.initialize(sinkTaskContext);
            myOkHttpSinkTask.start(settings);

            OkHttpClient httpClient = Mockito.mock(OkHttpClient.class);
            HttpExchange dummyHttpExchange = getHttpExchange();
            when(httpClient.call(any(HttpRequest.class), any(AtomicInteger.class))).thenReturn(CompletableFuture.supplyAsync(() -> dummyHttpExchange));
            when(httpClient.getEnrichRequestFunction()).thenReturn(request->request);
            when(httpClient.getAddSuccessStatusToHttpExchangeFunction()).thenReturn(new AddSuccessStatusToHttpExchangeFunction(defaultSuccessPattern));
            myOkHttpSinkTask.getDefaultConfiguration().getConfiguration().setHttpClient(httpClient);

            List<SinkRecord> records = Lists.newArrayList();
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, getDummyHttpRequestAsString(), -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord);
            myOkHttpSinkTask.put(records);

            verify(httpClient, times(1)).call(any(HttpRequest.class), any(AtomicInteger.class));
            List<ProducerRecord<String, Object>> history = mockProducer.history();
            assertThat(history).hasSize(1);
            ProducerRecord<String, Object> firstRecord = history.get(0);
            assertThat(firstRecord.topic()).isEqualTo("myTopic");
            assertThat(firstRecord.key()).isNull();
            assertThat(firstRecord.value()).isNotNull();
        }

        @Test
        void test_put_with_publish_to_in_memory_queue_set_to_true_with_a_consumer() {

            //given
            Map<String, String> settings = Maps.newHashMap();
            settings.put(PUBLISH_MODE, PublishMode.IN_MEMORY_QUEUE.name());
            QueueFactory.registerConsumerForQueue(QueueFactory.DEFAULT_QUEUE_NAME);
            ahcSinkTask.start(settings);
            AHCHttpClient httpClient = Mockito.mock(AHCHttpClient.class);
            HttpExchange dummyHttpExchange = getHttpExchange();
            when(httpClient.call(any(HttpRequest.class), any(AtomicInteger.class))).thenReturn(CompletableFuture.supplyAsync(() -> dummyHttpExchange));
            when(httpClient.getEnrichRequestFunction()).thenReturn(request->request);
            when(httpClient.getAddSuccessStatusToHttpExchangeFunction()).thenReturn(new AddSuccessStatusToHttpExchangeFunction(defaultSuccessPattern));
            ahcSinkTask.getDefaultConfiguration().getConfiguration().setHttpClient(httpClient);
            Queue<KafkaRecord> queue = mock(Queue.class);
            ahcSinkTask.setQueue(queue);
            List<SinkRecord> records = Lists.newArrayList();
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, getDummyHttpRequestAsString(), -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord);
            //when
            ahcSinkTask.put(records);

            //then
            verify(httpClient, times(1)).call(any(HttpRequest.class), any(AtomicInteger.class));
            verify(queue, times(1)).offer(any(KafkaRecord.class));
        }


    }

    @Nested
    class PutWithHttpRequestMapper {
        @Test
        void test_with_multiple_direct_http_request_mappers() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                settings.put(REQUEST_MAPPER_DEFAULT_MODE, MapperMode.DIRECT.name());
                settings.put(HTTP_REQUEST_MAPPER_IDS, "myid1,myid2");
                settings.put("http.request.mapper.myid1.mode", MapperMode.DIRECT.name());
                settings.put("http.request.mapper.myid1.matcher", "sinkRecord.topic()=='myTopic'");
                settings.put("http.request.mapper.myid2.mode", MapperMode.DIRECT.name());
                settings.put("http.request.mapper.myid2.matcher", "sinkRecord.topic()=='myTopic2'");


                okHttpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path1", "POST", DUMMY_BODY),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path2", "POST", DUMMY_BODY),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path3", "POST", DUMMY_BODY),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord3);

                //define the http Mock Server interaction
                WireMock wireMock = wmRuntimeInfo.getWireMock();
                String bodyResponse = "{\"result\":\"pong\"}";
                wireMock
                        .register(WireMock.post("/path1")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/path2")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/path3")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                //when

                okHttpSinkTask.put(records);
                wireMock.verifyThat(postRequestedFor(urlEqualTo("/path1")));
                wireMock.verifyThat(postRequestedFor(urlEqualTo("/path2")));
                wireMock.verifyThat(postRequestedFor(urlEqualTo("/path3")));


            });

        }

        @Test
        void test_with_multiple_http_request_mappers_direct_and_jexl() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                settings.put(REQUEST_MAPPER_DEFAULT_MODE, MapperMode.DIRECT.name());
                settings.put(HTTP_REQUEST_MAPPER_IDS, "myid1,myid2");
                settings.put("http.request.mapper.myid1.mode", MapperMode.JEXL.name());
                settings.put("http.request.mapper.myid1.matcher", "sinkRecord.topic()=='myTopic'");
                settings.put("http.request.mapper.myid1.url", "sinkRecord.value().split(\"#\")[0]");
                settings.put("http.request.mapper.myid1.method", "sinkRecord.value().split(\"#\")[1]");
                settings.put("http.request.mapper.myid1.bodytype", "'STRING'");
                settings.put("http.request.mapper.myid1.body", "sinkRecord.value().split(\"#\")[2]");
                settings.put("http.request.mapper.myid1.headers", "{'test1':['value1','value2',...]}");
                settings.put("http.request.mapper.myid2.mode", MapperMode.DIRECT.name());
                settings.put("http.request.mapper.myid2.matcher", "sinkRecord.topic()=='myTopic2'");


                okHttpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        "http://localhost:" + wmRuntimeInfo.getHttpPort() + "/path1" + "#" + "POST" + "#" + "body1",
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        "http://localhost:" + wmRuntimeInfo.getHttpPort() + "/path2" + "#" + "POST" + "#" + "body2",
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        "http://localhost:" + wmRuntimeInfo.getHttpPort() + "/path3" + "#" + "POST" + "#" + "body3",
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord3);

                //define the http Mock Server interaction
                WireMock wireMock = wmRuntimeInfo.getWireMock();
                String bodyResponse = "{\"result\":\"pong\"}";
                wireMock
                        .register(WireMock.post("/path1")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/path2")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/path3")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                //when

                okHttpSinkTask.put(records);
                wireMock.verifyThat(postRequestedFor(urlEqualTo("/path1")));
                wireMock.verifyThat(postRequestedFor(urlEqualTo("/path2")));
                wireMock.verifyThat(postRequestedFor(urlEqualTo("/path3")));
            });
        }
    }

    @Nested
    class MessageSplitter {
        @Test
        void test_message_splitter_with_split_pattern() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                settings.put(MESSAGE_SPLITTER_IDS, "myid1");
                settings.put("message.splitter.myid1.matcher", "sinkRecord.topic()=='myTopic'");
                settings.put("message.splitter.myid1.pattern", "\n");
                settings.put(DEFAULT_REQUEST_MAPPER_PREFIX + REQUEST_MAPPER_DEFAULT_MODE, MapperMode.JEXL.name());
                settings.put(DEFAULT_REQUEST_MAPPER_PREFIX + REQUEST_MAPPER_DEFAULT_URL_EXPRESSION, "sinkRecord.value()");
                settings.put(DEFAULT_REQUEST_MAPPER_PREFIX + REQUEST_MAPPER_DEFAULT_METHOD_EXPRESSION, "'GET'");
                okHttpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        "http://localhost:" + wmRuntimeInfo.getHttpPort() + "/path1" + "\n" +
                                "http://localhost:" + wmRuntimeInfo.getHttpPort() + "/path2" + "\n" +
                                "http://localhost:" + wmRuntimeInfo.getHttpPort() + "/path3",
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);

                //define the http Mock Server interaction
                WireMock wireMock = wmRuntimeInfo.getWireMock();
                String bodyResponse = "{\"result\":\"pong\"}";
                wireMock
                        .register(WireMock.get("/path1")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.get("/path2")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.get("/path3")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );

                //when
                okHttpSinkTask.put(records);
                //then
                wireMock.verifyThat(getRequestedFor(urlEqualTo("/path1")));
                wireMock.verifyThat(getRequestedFor(urlEqualTo("/path2")));
                wireMock.verifyThat(getRequestedFor(urlEqualTo("/path3")));


            });

        }

        @Test
        void test_message_splitter_with_split_pattern_and_limit() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                settings.put(DEFAULT_REQUEST_MAPPER_PREFIX + REQUEST_MAPPER_DEFAULT_MODE, MapperMode.JEXL.name());
                settings.put(DEFAULT_REQUEST_MAPPER_PREFIX + REQUEST_MAPPER_DEFAULT_URL_EXPRESSION, "sinkRecord.value()");
                settings.put(DEFAULT_REQUEST_MAPPER_PREFIX + REQUEST_MAPPER_DEFAULT_METHOD_EXPRESSION, "'GET'");
                settings.put(MESSAGE_SPLITTER_IDS, "myid1");
                settings.put("message.splitter.myid1.matcher", "sinkRecord.topic()=='myTopic'");
                settings.put("message.splitter.myid1.pattern", "\\|");
                settings.put("message.splitter.myid1.limit", "2");


                okHttpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        "http://localhost:" + wmRuntimeInfo.getHttpPort() + "/path1" + "|" +
                                "http://localhost:" + wmRuntimeInfo.getHttpPort() + "/path2" + "|" +
                                "test",
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                //define the http Mock Server interaction
                WireMock wireMock = wmRuntimeInfo.getWireMock();
                String bodyResponse = "{\"result\":\"pong\"}";
                wireMock
                        .register(WireMock.get("/path1")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.get("/path2|test")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );

                //when
                okHttpSinkTask.put(records);
                //then
                wireMock.verifyThat(getRequestedFor(urlEqualTo("/path1")));
                //%7C is the encoded version of the '|' character
                wireMock.verifyThat(getRequestedFor(urlEqualTo("/path2%7Ctest")));


            });

        }
    }

    @Nested
    class RequestGrouper{
        @Test
        void test_request_grouper_minimal_settings() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                String id = "myid1";
                settings.put(REQUEST_GROUPER_IDS, id);
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+URL_REGEX, ".*");
                okHttpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                String value1= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff1",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value1.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                String value2= """
       {
          "url": "http://localhost:PORT/path1",
          "headers": {
            "X-request-id": [
              "aaaa-4466666-111"
            ],
            "X-correlation-id": [
              "sfds-55-77"
            ]
          },
          "method": "POST",
          "bodyAsString": "stuff2",
          "bodyType": "STRING"
        }
""";
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value2.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()) ,
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                String value3= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff3",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value3.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord3);

                //define the http Mock Server interaction
                WireMock wireMock = wmRuntimeInfo.getWireMock();
                String bodyResponse = "{\"result\":\"pong\"}";
                wireMock
                        .register(WireMock.post("/path1")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );

                //when
                okHttpSinkTask.put(records);
                //then
                wireMock.verifyThat(postRequestedFor(
                        urlEqualTo("/path1"))
                        .withRequestBody(equalTo("stuff1stuff2stuff3"))
                );
            });

        }
        @Test
        void test_request_grouper_with_start() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                String id = "myid1";
                settings.put(REQUEST_GROUPER_IDS, id);
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+URL_REGEX, ".*");
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+"start", "hello\n");
                okHttpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                String value1= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff1",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value1.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                String value2= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff2",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value2.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()) ,
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                String value3= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff3",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value3.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord3);

                //define the http Mock Server interaction
                WireMock wireMock = wmRuntimeInfo.getWireMock();
                String bodyResponse = "{\"result\":\"pong\"}";
                wireMock
                        .register(WireMock.post("/path1")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );

                //when
                okHttpSinkTask.put(records);
                //then
                wireMock.verifyThat(postRequestedFor(
                        urlEqualTo("/path1"))
                        .withRequestBody(equalTo("hello\nstuff1stuff2stuff3"))
                );
            });

        }
        @Test
        void test_request_grouper_with_end() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                String id = "myid1";
                settings.put(REQUEST_GROUPER_IDS, id);
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+URL_REGEX, ".*");
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+"end", "\nhello");
                okHttpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                String value1= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff1",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value1.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                String value2= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff2",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value2.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()) ,
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                String value3= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff3",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value3.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord3);

                //define the http Mock Server interaction
                WireMock wireMock = wmRuntimeInfo.getWireMock();
                String bodyResponse = "{\"result\":\"pong\"}";
                wireMock
                        .register(WireMock.post("/path1")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );

                //when
                okHttpSinkTask.put(records);
                //then
                wireMock.verifyThat(postRequestedFor(
                        urlEqualTo("/path1"))
                        .withRequestBody(equalTo("stuff1stuff2stuff3\nhello"))
                );
            });

        }

        @Test
        void test_request_grouper_with_start_and_end() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                String id = "myid1";
                settings.put(REQUEST_GROUPER_IDS, id);
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+URL_REGEX, ".*");
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+"start", "hello\n");
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+"end", "\ngood bye");
                okHttpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                String value1= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff1",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value1.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                String value2= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff2",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value2.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()) ,
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                String value3= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff3",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value3.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord3);

                //define the http Mock Server interaction
                WireMock wireMock = wmRuntimeInfo.getWireMock();
                String bodyResponse = "{\"result\":\"pong\"}";
                wireMock
                        .register(WireMock.post("/path1")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );

                //when
                okHttpSinkTask.put(records);
                //then
                wireMock.verifyThat(postRequestedFor(
                        urlEqualTo("/path1"))
                        .withRequestBody(equalTo("hello\nstuff1stuff2stuff3\ngood bye"))
                );
            });

        }

        @Test
        void test_request_grouper_with_start_and_end_and_separator() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                String id = "myid1";
                settings.put(REQUEST_GROUPER_IDS, id);
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+URL_REGEX, ".*");
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+"start", "hello\n");
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+"end", "\ngood bye");
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+"separator", "#");
                okHttpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                String value1= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff1",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value1.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                String value2= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff2",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value2.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()) ,
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                String value3= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff3",
                                  "bodyType": "STRING"
                                }
                        
                        """;
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value3.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord3);

                //define the http Mock Server interaction
                WireMock wireMock = wmRuntimeInfo.getWireMock();
                String bodyResponse = "{\"result\":\"pong\"}";
                wireMock
                        .register(WireMock.post("/path1")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );

                //when
                okHttpSinkTask.put(records);
                //then
                wireMock.verifyThat(postRequestedFor(
                        urlEqualTo("/path1"))
                        .withRequestBody(equalTo("hello\nstuff1#stuff2#stuff3\ngood bye"))
                );
            });

        }

        @Test
        void test_request_grouper_with_separator() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                String id = "myid1";
                settings.put(REQUEST_GROUPER_IDS, id);
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+URL_REGEX, ".*");
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+"separator", "#");
                okHttpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                String value1= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff1",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value1.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                String value2= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff2",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value2.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()) ,
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                String value3= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff3",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value3.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord3);

                //define the http Mock Server interaction
                WireMock wireMock = wmRuntimeInfo.getWireMock();
                String bodyResponse = "{\"result\":\"pong\"}";
                wireMock
                        .register(WireMock.post("/path1")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );

                //when
                okHttpSinkTask.put(records);
                //then
                wireMock.verifyThat(postRequestedFor(
                        urlEqualTo("/path1"))
                        .withRequestBody(equalTo("stuff1#stuff2#stuff3"))
                );
            });

        }

        @Test
        void test_request_grouper_with_separator_and_non_matching_messages() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                String id = "myid1";
                settings.put(REQUEST_GROUPER_IDS, id);
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+URL_REGEX, ".*/path1");
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+"separator", "#");
                okHttpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                String value1= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff1",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value1.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                String nonMatchingValue= """
                                {
                                  "url": "http://localhost:PORT/pathA",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuffA",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord nonMatchingSinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        nonMatchingValue.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(nonMatchingSinkRecord);
                String value2= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff2",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value2.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()) ,
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                String value3= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff3",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value3.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord3);

                //define the http Mock Server interaction
                WireMock wireMock = wmRuntimeInfo.getWireMock();
                String bodyResponse = "{\"result\":\"pong\"}";
                wireMock
                        .register(WireMock.post("/path1")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/pathA")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );

                //when
                okHttpSinkTask.put(records);
                //then
                wireMock.verifyThat(postRequestedFor(
                        urlEqualTo("/path1"))
                        .withRequestBody(equalTo("stuff1#stuff2#stuff3"))
                );
                wireMock.verifyThat(postRequestedFor(
                        urlEqualTo("/pathA"))
                        .withRequestBody(equalTo("stuffA"))
                );

            });

        }

        @Test
        void test_multiple_request_groupers_with_separator_and_non_matching_messages() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                String id = "myid1";
                String id2 = "myid2";
                settings.put(REQUEST_GROUPER_IDS, id+","+id2);
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+URL_REGEX, ".*/path1");
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+"separator", "#");
                settings.put(REQUEST_GROUPER_PREFIX+id2+"."+URL_REGEX, ".*/pathB");
                settings.put(REQUEST_GROUPER_PREFIX+id2+"."+"separator", "#");
                okHttpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                String value1= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff1",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value1.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                String nonMatchingValue= """
                                {
                                  "url": "http://localhost:PORT/pathA",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuffA",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord nonMatchingSinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        nonMatchingValue.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(nonMatchingSinkRecord);
                String value2= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff2",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value2.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()) ,
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                String value3= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff3",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value3.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord3);

                //define the http Mock Server interaction
                WireMock wireMock = wmRuntimeInfo.getWireMock();
                String bodyResponse = "{\"result\":\"pong\"}";
                wireMock
                        .register(WireMock.post("/path1")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/pathA")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );

                //when
                okHttpSinkTask.put(records);
                //then
                wireMock.verifyThat(postRequestedFor(
                        urlEqualTo("/path1"))
                        .withRequestBody(equalTo("stuff1#stuff2#stuff3"))
                );
                wireMock.verifyThat(postRequestedFor(
                        urlEqualTo("/pathA"))
                        .withRequestBody(equalTo("stuffA"))
                );

            });

        }

        @Test
        void test_request_grouper_with_separator_and_more_non_matching_messages() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                String id = "myid1";
                settings.put(REQUEST_GROUPER_IDS, id);
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+URL_REGEX, ".*/path1");
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+"separator", "#");
                okHttpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                String value1= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff1",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value1.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                String nonMatchingValue= """
                                {
                                  "url": "http://localhost:PORT/pathA",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuffA",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord nonMatchingSinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        nonMatchingValue.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(nonMatchingSinkRecord);

                String value2= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff2",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value2.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()) ,
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                String value3= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff3",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value3.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord3);
                String nonMatchingValue2= """
                                {
                                  "url": "http://localhost:PORT/pathA",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuffA",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord nonMatchingSinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        nonMatchingValue2.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(nonMatchingSinkRecord2);
                //define the http Mock Server interaction
                WireMock wireMock = wmRuntimeInfo.getWireMock();
                String bodyResponse = "{\"result\":\"pong\"}";
                wireMock
                        .register(WireMock.post("/path1")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/pathA")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );

                //when
                okHttpSinkTask.put(records);
                //then
                wireMock.verifyThat(postRequestedFor(
                        urlEqualTo("/path1"))
                        .withRequestBody(equalTo("stuff1#stuff2#stuff3"))
                );
                wireMock.verifyThat(2,postRequestedFor(
                        urlEqualTo("/pathA"))
                        .withRequestBody(equalTo("stuffA"))
                );

            });

        }


        @Test
        void test_request_grouper_with_message_limit() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                String id = "myid1";
                settings.put(REQUEST_GROUPER_IDS, id);
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+URL_REGEX, ".*");
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+"message.limit", "2");
                okHttpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                String value1= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff1",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value1.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                String value2= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff2",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value2.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()) ,
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                String value3= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff3",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value3.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord3);

                //define the http Mock Server interaction
                WireMock wireMock = wmRuntimeInfo.getWireMock();
                String bodyResponse = "{\"result\":\"pong\"}";
                wireMock
                        .register(WireMock.post("/path1")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );

                //when
                okHttpSinkTask.put(records);
                //then
                wireMock.verifyThat(postRequestedFor(
                        urlEqualTo("/path1"))
                        .withRequestBody(equalTo("stuff1stuff2"))
                );
                wireMock.verifyThat(postRequestedFor(
                        urlEqualTo("/path1"))
                        .withRequestBody(equalTo("stuff3"))
                );
            });

        }

        @Test
        void test_request_grouper_with_start_and_end_and_separator_and_message_limit() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                String id = "myid1";
                settings.put(REQUEST_GROUPER_IDS, id);
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+URL_REGEX, ".*");
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+"start", "hello\n");
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+"end", "\ngood bye");
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+"separator", "#");
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+"message.limit", "2");
                okHttpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                String value1= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff1",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value1.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                String value2= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff2",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value2.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()) ,
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                String value3= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff3",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value3.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord3);

                //define the http Mock Server interaction
                WireMock wireMock = wmRuntimeInfo.getWireMock();
                String bodyResponse = "{\"result\":\"pong\"}";
                wireMock
                        .register(WireMock.post("/path1")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );

                //when
                okHttpSinkTask.put(records);
                //then
                wireMock.verifyThat(postRequestedFor(
                        urlEqualTo("/path1"))
                        .withRequestBody(equalTo("hello\nstuff1#stuff2\ngood bye"))
                );
                wireMock.verifyThat(postRequestedFor(
                        urlEqualTo("/path1"))
                        .withRequestBody(equalTo("hello\nstuff3\ngood bye"))
                );
            });

        }

        @Test
        void test_request_grouper_with_body_limit() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                String id = "myid1";
                settings.put(REQUEST_GROUPER_IDS, id);
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+URL_REGEX, ".*");
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+"body.limit", "21");
                okHttpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                String value1= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "1234567890",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value1.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                String value2= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "1234567890",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value2.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()) ,
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                String value3= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "1234567890",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value3.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord3);

                //define the http Mock Server interaction
                WireMock wireMock = wmRuntimeInfo.getWireMock();
                String bodyResponse = "{\"result\":\"pong\"}";
                wireMock
                        .register(WireMock.post("/path1")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );

                //when
                okHttpSinkTask.put(records);
                //then
                wireMock.verifyThat(postRequestedFor(
                        urlEqualTo("/path1"))
                        .withRequestBody(equalTo("12345678901234567890"))
                );
                wireMock.verifyThat(postRequestedFor(
                        urlEqualTo("/path1"))
                        .withRequestBody(equalTo("1234567890"))
                );
            });

        }

        @Test
        void test_request_grouper_with_start_and_end_and_separator_and_body_limit() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                String id = "myid1";
                settings.put(REQUEST_GROUPER_IDS, id);
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+URL_REGEX, ".*");
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+"start", "hello\n");
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+"end", "\ngood bye");
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+"separator", "#");
                settings.put(REQUEST_GROUPER_PREFIX+id+"."+"body.limit", "25");
                okHttpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                String value1= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff1",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value1.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                String value2= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff2",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value2.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()) ,
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                String value3= """
                                {
                                  "url": "http://localhost:PORT/path1",
                                  "headers": {
                                    "X-request-id": [
                                      "aaaa-4466666-111"
                                    ],
                                    "X-correlation-id": [
                                      "sfds-55-77"
                                    ]
                                  },
                                  "method": "POST",
                                  "bodyAsString": "stuff3",
                                  "bodyType": "STRING"
                                }
                        """;
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        value3.replaceFirst("PORT",""+wmRuntimeInfo.getHttpPort()),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord3);

                //define the http Mock Server interaction
                WireMock wireMock = wmRuntimeInfo.getWireMock();
                String bodyResponse = "{\"result\":\"pong\"}";
                wireMock
                        .register(WireMock.post("/path1")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );

                //when
                okHttpSinkTask.put(records);
                //then
                wireMock.verifyThat(postRequestedFor(
                        urlEqualTo("/path1"))
                        .withRequestBody(equalTo("hello\nstuff1#stuff2\ngood bye"))
                );
                wireMock.verifyThat(postRequestedFor(
                        urlEqualTo("/path1"))
                        .withRequestBody(equalTo("hello\nstuff3\ngood bye"))
                );
            });

        }
    }

    @Nested
    class PutWithMeterRegistry {

        @BeforeEach
        void setup(){
            OkHttpSinkTask.clearMeterRegistry();
        }

        @Test
        void test_meter_registry_activate_jmx() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                settings.put(METER_REGISTRY_EXPORTER_JMX_ACTIVATE, "true");
                okHttpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path1", "POST", DUMMY_BODY),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path2", "POST", DUMMY_BODY),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path3", "POST", DUMMY_BODY),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord3);

                //define the http Mock Server interaction
                WireMock wireMock = wmRuntimeInfo.getWireMock();
                String bodyResponse = "{\"result\":\"pong\"}";
                wireMock
                        .register(WireMock.post("/path1")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/path2")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/path3")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                //when

                okHttpSinkTask.put(records);


                CompositeMeterRegistry meterRegistry = HttpTask.getMeterRegistry();
                Set<MeterRegistry> registries = meterRegistry.getRegistries();
                assertThat(registries).hasSize(1);
                List<MeterRegistry> meterRegistryList = Arrays.asList(registries.toArray(new MeterRegistry[0]));
                MeterRegistry meterRegistry1 = meterRegistryList.get(0);
                assertThat(JmxMeterRegistry.class.isAssignableFrom(meterRegistry1.getClass()));
                List<Meter> meters = meterRegistry1.getMeters();
                assertThat(meters).isNotEmpty();
                for (Meter meter : meters) {
                    LOGGER.info("meter : {}", meter.getId());
                }
            });
        }

        @Test
        void test_meter_registry_activate_prometheus() {
            int availablePort = getRandomPort();
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                settings.put(METER_REGISTRY_EXPORTER_PROMETHEUS_ACTIVATE, "true");
                settings.put(METER_REGISTRY_EXPORTER_PROMETHEUS_PORT, availablePort + "");
                okHttpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path1", "POST", DUMMY_BODY),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path2", "POST", DUMMY_BODY),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path3", "POST", DUMMY_BODY),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord3);

                //define the http Mock Server interaction
                WireMock wireMock = wmRuntimeInfo.getWireMock();
                String bodyResponse = "{\"result\":\"pong\"}";
                wireMock
                        .register(WireMock.post("/path1")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/path2")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/path3")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                //when

                okHttpSinkTask.put(records);


                CompositeMeterRegistry meterRegistry = HttpTask.getMeterRegistry();
                Set<MeterRegistry> registries = meterRegistry.getRegistries();
                assertThat(registries).hasSize(1);
                List<MeterRegistry> meterRegistryList = Arrays.asList(registries.toArray(new MeterRegistry[0]));
                MeterRegistry meterRegistry1 = meterRegistryList.get(0);
                assertThat(JmxMeterRegistry.class.isAssignableFrom(meterRegistry1.getClass()));
                List<Meter> meters = meterRegistry1.getMeters();
                assertThat(meters).isNotEmpty();
                for (Meter meter : meters) {
                    LOGGER.info("meter : {}", meter.getId());
                }
            });
        }

        @Test
        void test_meter_registry_activate_jmx_and_prometheus() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                settings.put(METER_REGISTRY_EXPORTER_JMX_ACTIVATE, "true");
                settings.put(METER_REGISTRY_EXPORTER_PROMETHEUS_ACTIVATE, "true");
                settings.put(METER_REGISTRY_EXPORTER_PROMETHEUS_PORT, "9090");
                okHttpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path1", "POST", DUMMY_BODY),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path2", "POST", DUMMY_BODY),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path3", "POST", DUMMY_BODY),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord3);

                //define the http Mock Server interaction
                WireMock wireMock = wmRuntimeInfo.getWireMock();
                String bodyResponse = "{\"result\":\"pong\"}";
                wireMock
                        .register(WireMock.post("/path1")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/path2")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/path3")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                //when

                okHttpSinkTask.put(records);


                CompositeMeterRegistry meterRegistry = HttpTask.getMeterRegistry();
                Set<MeterRegistry> registries = meterRegistry.getRegistries();
                assertThat(registries).hasSize(2);
                List<MeterRegistry> meterRegistryList = Arrays.asList(registries.toArray(new MeterRegistry[0]));
                MeterRegistry meterRegistry1 = meterRegistryList.get(0);
                assertThat(JmxMeterRegistry.class.isAssignableFrom(meterRegistry1.getClass()) || PrometheusMeterRegistry.class.isAssignableFrom(meterRegistry1.getClass()));
                MeterRegistry meterRegistry2 = meterRegistryList.get(1);
                assertThat(PrometheusMeterRegistry.class.isAssignableFrom(meterRegistry2.getClass()) || JmxMeterRegistry.class.isAssignableFrom(meterRegistry2.getClass()));
                List<Meter> meters = meterRegistry1.getMeters();
                assertThat(meters).isNotEmpty();
                for (Meter meter : meters) {
                    LOGGER.info("meter : {}", meter.getId());
                }
            });
        }

        @Test
        void test_meter_registry_activate_jmx_and_prometheus_with_all_bindings() {
            int availablePort = getRandomPort();
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                settings.put(METER_REGISTRY_EXPORTER_JMX_ACTIVATE, "true");
                settings.put(METER_REGISTRY_EXPORTER_PROMETHEUS_ACTIVATE, "true");
                settings.put(METER_REGISTRY_EXPORTER_PROMETHEUS_PORT, "" + availablePort);
                settings.put(METER_REGISTRY_BIND_METRICS_EXECUTOR_SERVICE, "true");
                settings.put(METER_REGISTRY_BIND_METRICS_JVM_MEMORY, "true");
                settings.put(METER_REGISTRY_BIND_METRICS_JVM_THREAD, "true");
                settings.put(METER_REGISTRY_BIND_METRICS_JVM_INFO, "true");
                settings.put(METER_REGISTRY_BIND_METRICS_JVM_GC, "true");
                settings.put(METER_REGISTRY_BIND_METRICS_JVM_CLASSLOADER, "true");
                settings.put(METER_REGISTRY_BIND_METRICS_JVM_PROCESSOR, "true");
                settings.put(METER_REGISTRY_BIND_METRICS_LOGBACK, "true");
                okHttpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path1", "POST", DUMMY_BODY),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path2", "POST", DUMMY_BODY),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path3", "POST", DUMMY_BODY),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord3);

                //define the http Mock Server interaction
                WireMock wireMock = wmRuntimeInfo.getWireMock();
                String bodyResponse = "{\"result\":\"pong\"}";
                wireMock
                        .register(WireMock.post("/path1")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/path2")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/path3")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage(OK)
                                        .withFixedDelay(1000)
                                )
                        );
                //when

                okHttpSinkTask.put(records);


                CompositeMeterRegistry meterRegistry = HttpTask.getMeterRegistry();
                Set<MeterRegistry> registries = meterRegistry.getRegistries();
                assertThat(registries).hasSize(2);
                List<MeterRegistry> meterRegistryList = Arrays.asList(registries.toArray(new MeterRegistry[0]));
                MeterRegistry meterRegistry1 = meterRegistryList.get(0);
                assertThat(JmxMeterRegistry.class.isAssignableFrom(meterRegistry1.getClass()) || PrometheusMeterRegistry.class.isAssignableFrom(meterRegistry1.getClass()));
                MeterRegistry meterRegistry2 = meterRegistryList.get(1);
                assertThat(PrometheusMeterRegistry.class.isAssignableFrom(meterRegistry2.getClass()) || JmxMeterRegistry.class.isAssignableFrom(meterRegistry2.getClass()));
                List<Meter> meters = meterRegistry1.getMeters();
                assertThat(meters).isNotEmpty();
                for (Meter meter : meters) {
                    LOGGER.info("meter : {}", meter.getId());
                }
            });
        }
    }

    @Nested
    class PutWithLatencies {
        @Test
        @DisplayName("test with multiple http requests with slow responses with AHC implementation, expected ok")
        void test_put_with_latencies_and_ahc_implementation() {
            //given
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();

            Map<String, String> settings = Maps.newHashMap();
            settings.put(CONFIG_DEFAULT_RATE_LIMITER_MAX_EXECUTIONS, "100");
            settings.put(CONFIG_HTTP_CLIENT_IMPLEMENTATION, AHC_IMPLEMENTATION);
            okHttpSinkTask.start(settings);


            //init sinkRecord
            List<SinkRecord> records = Lists.newArrayList();
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path1", "POST", DUMMY_BODY),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord1);
            SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path2", "POST", DUMMY_BODY),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord2);
            SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path3", "POST", DUMMY_BODY),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord3);

            //define the http Mock Server interaction
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            String bodyResponse = "{\"result\":\"pong\"}";
            wireMock
                    .register(WireMock.post("/path1")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage(OK)
                                    .withFixedDelay(1000)
                            )
                    );
            wireMock
                    .register(WireMock.post("/path2")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage(OK)
                                    .withFixedDelay(1000)
                            )
                    );
            wireMock
                    .register(WireMock.post("/path3")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage(OK)
                                    .withFixedDelay(1000)
                            )
                    );
            //when
            Stopwatch stopwatch = Stopwatch.createStarted();
            okHttpSinkTask.put(records);
            stopwatch.stop();
            long elapsedMillis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            LOGGER.info("put method execution time :'{}' ms", elapsedMillis);
            //then
            assertThat(elapsedMillis).isLessThan(2990);

        }

        @Test
        @DisplayName("test with multiple http requests with slow responses with OKHttp implementation, expected ok")
        void test_put_with_latencies_and_ok_http_implementation() {

            int availableProcessors = Runtime.getRuntime().availableProcessors();
            LOGGER.info("availableProcessors:{}", availableProcessors);
            //given
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();

            Map<String, String> settings = Maps.newHashMap();
            settings.put(CONFIG_DEFAULT_RATE_LIMITER_MAX_EXECUTIONS, "100");
            settings.put(CONFIG_HTTP_CLIENT_IMPLEMENTATION, OKHTTP_IMPLEMENTATION);
            okHttpSinkTask.start(settings);


            //init sinkRecord
            List<SinkRecord> records = Lists.newArrayList();
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path1", "POST", DUMMY_BODY),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord1);
            SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path2", "POST", DUMMY_BODY),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord2);
            SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path3", "POST", DUMMY_BODY),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord3);

            //define the http Mock Server interaction
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            String bodyResponse = "{\"result\":\"pong\"}";
            wireMock
                    .register(WireMock.post("/path1")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage(OK)
                                    .withFixedDelay(1000)
                            )
                    );
            wireMock
                    .register(WireMock.post("/path2")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage(OK)
                                    .withFixedDelay(1000)
                            )
                    );
            wireMock
                    .register(WireMock.post("/path3")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage(OK)
                                    .withFixedDelay(1000)
                            )
                    );
            //when
            Stopwatch stopwatch = Stopwatch.createStarted();

            okHttpSinkTask.put(records);
            stopwatch.stop();
            long elapsedMillis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            LOGGER.info("put method execution time :'{}' ms", elapsedMillis);
            //then
            assertThat(elapsedMillis).isLessThan(2800);

        }

        @Test
        @DisplayName("test with multiple http requests with slow responses with AHC implementation with fixed thread pool, expected ok")
        void test_put_with_latencies_and_ahc_implementation_and_fixed_thread_pool() {
            //given
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();

            Map<String, String> settings = Maps.newHashMap();
            settings.put(CONFIG_DEFAULT_RATE_LIMITER_MAX_EXECUTIONS, "100");
            settings.put(CONFIG_HTTP_CLIENT_IMPLEMENTATION, AHC_IMPLEMENTATION);
            settings.put(HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE, "4");

            okHttpSinkTask.start(settings);


            //init sinkRecord
            List<SinkRecord> records = Lists.newArrayList();
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path1", "POST", DUMMY_BODY),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord1);
            SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path2", "POST", DUMMY_BODY),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord2);
            SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path3", "POST", DUMMY_BODY),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord3);

            //define the http Mock Server interaction
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            String bodyResponse = "{\"result\":\"pong\"}";
            wireMock
                    .register(WireMock.post("/path1")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage(OK)
                                    .withFixedDelay(1000)
                            )
                    );
            wireMock
                    .register(WireMock.post("/path2")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage(OK)
                                    .withFixedDelay(1000)
                            )
                    );
            wireMock
                    .register(WireMock.post("/path3")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage(OK)
                                    .withFixedDelay(1000)
                            )
                    );
            //when
            Stopwatch stopwatch = Stopwatch.createStarted();
            okHttpSinkTask.put(records);
            okHttpSinkTask.stop();
            stopwatch.stop();
            long elapsedMillis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            LOGGER.info("put method execution time :'{}' ms", elapsedMillis);
            //then
            assertThat(elapsedMillis).isLessThan(2800);

        }

        @Test
        @DisplayName("test with multiple http requests with slow responses with OKHttp implementation and fixed thread pool, expected ok")
        void test_put_with_latencies_and_ok_http_implementation_with_fixed_thread_pool() {

            int availableProcessors = Runtime.getRuntime().availableProcessors();
            LOGGER.info("availableProcessors:{}", availableProcessors);
            //given
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();

            Map<String, String> settings = Maps.newHashMap();
            settings.put(CONFIG_DEFAULT_RATE_LIMITER_MAX_EXECUTIONS, "100");
            settings.put(CONFIG_HTTP_CLIENT_IMPLEMENTATION, OKHTTP_IMPLEMENTATION);
            settings.put(HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE, "" + 2);
            okHttpSinkTask.start(settings);


            //init sinkRecord
            List<SinkRecord> records = Lists.newArrayList();
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path1", "POST", DUMMY_BODY),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord1);
            SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path2", "POST", DUMMY_BODY),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord2);
            SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path3", "POST", DUMMY_BODY),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord3);

            //define the http Mock Server interaction
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            String bodyResponse = "{\"result\":\"pong\"}";
            wireMock
                    .register(WireMock.post("/path1")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage(OK)
                                    .withFixedDelay(1000)
                            )
                    );
            wireMock
                    .register(WireMock.post("/path2")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage(OK)
                                    .withFixedDelay(1000)
                            )
                    );
            wireMock
                    .register(WireMock.post("/path3")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage(OK)
                                    .withFixedDelay(1000)
                            )
                    );
            //when
            Stopwatch stopwatch = Stopwatch.createStarted();

            okHttpSinkTask.put(records);
            stopwatch.stop();
            long elapsedMillis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            LOGGER.info("put method execution time :'{}' ms", elapsedMillis);
            //then
            assertThat(elapsedMillis).isLessThan(2950);

        }


    }

    @Nested
    class PutWithAuthentication {

        @Test
        void test_oauth2_authentication_client_credentials_flow_with_basic_auth_to_get_token() throws IOException {
            //given
            final String WELL_KNOWN_OPENID_CONFIGURATION = "/.well-known/openid-configuration";
            final String WELL_KNOWN_OK = "WellKnownOk";
            final String UNAUTHORIZED = "Unauthorized";
            final String TOKEN_OK = "TokenOk";
            final String SONG_OK = "SongOk";
            final String SONG_PATH = "/v1/tracks/2TpxZ7JUBn3uw46aR7qd6V";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            String httpBaseUrl = wmRuntimeInfo.getHttpBaseUrl();
            Map<String, String> settings = Maps.newHashMap();
            settings.put(CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE, "true");
            settings.put(CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL, httpBaseUrl + "/.well-known/openid-configuration");
            settings.put(CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID, "44d34a4d05344c97837d463207805f8b");
            settings.put(CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET, "3fc0576720544ac293a3a5304e6c0fa8");
            settings.put(HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE, "1");


            //init sinkRecord
            List<SinkRecord> records = Lists.newArrayList();
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), SONG_PATH, "GET", null),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord1);
            SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), SONG_PATH, "GET", null),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord2);
            SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), SONG_PATH, "GET", null),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord3);

            //define the http Mock Server interaction
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            Path path = Paths.get("src/test/resources/oauth2/wellknownUrlContent.json");
            httpBaseUrl = wmRuntimeInfo.getHttpBaseUrl();
            String content = Files.readString(path);
            String wellKnownUrlContent = content.replaceAll("baseUrl", httpBaseUrl);
            String scenario = "test_oauth2_authentication_client_credentials_flow_with_basic_auth_to_get_token";
            //good well known content
            wireMock
                    .register(WireMock.get(WELL_KNOWN_OPENID_CONFIGURATION)
                            .inScenario(scenario)
                            .whenScenarioStateIs(STARTED)
                            .willReturn(WireMock.aResponse()
                                    .withStatus(200)
                                    .withStatusMessage(OK)
                                    .withBody(wellKnownUrlContent)
                            ).willSetStateTo(WELL_KNOWN_OK)
                    );
            Path tokenPath = Paths.get("src/test/resources/oauth2/token.json");
            String tokenContent = Files.readString(tokenPath);

            wireMock
                    .register(
                            WireMock.post("/api/token")
                                    .inScenario(scenario)
                                    .withHeader("Content-Type", containing("application/x-www-form-urlencoded; charset=UTF-8"))
                                    .withHeader("Authorization", containing("Basic NDRkMzRhNGQwNTM0NGM5NzgzN2Q0NjMyMDc4MDVmOGI6M2ZjMDU3NjcyMDU0NGFjMjkzYTNhNTMwNGU2YzBmYTg="))
                                    .whenScenarioStateIs(UNAUTHORIZED)
                                    .willReturn(WireMock.aResponse()
                                            .withStatus(200)
                                            .withStatusMessage(OK)
                                            .withBody(tokenContent)
                                    ).willSetStateTo(TOKEN_OK)
                    );

            Path songPath = Paths.get("src/test/resources/oauth2/song.json");
            String songContent = Files.readString(songPath);
            wireMock
                    .register(
                            WireMock.get(SONG_PATH)
                                    .inScenario(scenario)
                                    .withHeader("Authorization", containing(BEARER_TOKEN))
                                    .whenScenarioStateIs(TOKEN_OK)
                                    .willReturn(WireMock.aResponse()
                                            .withStatus(200)
                                            .withStatusMessage(OK)
                                            .withBody(songContent)
                                    ).willSetStateTo(SONG_OK)
                    );
            wireMock
                    .register(
                            WireMock.get(SONG_PATH)
                                    .inScenario(scenario)
                                    .withHeader("Authorization", containing(BEARER_TOKEN))
                                    .whenScenarioStateIs(SONG_OK)
                                    .willReturn(WireMock.aResponse()
                                            .withStatus(200)
                                            .withStatusMessage(OK)
                                            .withBody(songContent)
                                    ).willSetStateTo(SONG_OK)
                    );
            wireMock
                    .register(
                            WireMock.get(SONG_PATH)
                                    .inScenario(scenario)
                                    .whenScenarioStateIs(WELL_KNOWN_OK)
                                    .willReturn(WireMock.aResponse()
                                            .withStatus(401)
                                            .withStatusMessage("Unauthorized") //401 + 'WWW-Authenticate' trigger challenge
                                            .withHeader("WWW-Authenticate", "Bearer")
                                            .withBody(songContent)
                                    ).willSetStateTo(UNAUTHORIZED)
                    );
            wireMock
                    .register(
                            WireMock.get(SONG_PATH)
                                    .inScenario(scenario)
                                    .whenScenarioStateIs(UNAUTHORIZED)
                                    .willReturn(WireMock.aResponse()
                                            .withStatus(401)
                                            .withStatusMessage("Unauthorized") //401 + 'WWW-Authenticate' trigger challenge
                                            .withHeader("WWW-Authenticate", "Bearer")
                                            .withBody(songContent)
                                    ).willSetStateTo(UNAUTHORIZED)
                    );

            //when
            okHttpSinkTask.start(settings);
            okHttpSinkTask.put(records);
            //then
            wireMock.verifyThat(3, getRequestedFor(urlEqualTo(SONG_PATH)).withHeader("Authorization", equalTo(BEARER_TOKEN)));

        }

        @Test
        void test_oauth2_authentication_client_credentials_flow_with_basic_auth_to_get_token_and_expiration() throws IOException {
            //given
            final String WELL_KNOWN_OPENID_CONFIGURATION = "/.well-known/openid-configuration";
            final String WELL_KNOWN_OK = "WellKnownOk";
            final String UNAUTHORIZED = "Unauthorized";
            final String TOKEN_OK = "TokenOk";
            final String TOKEN_OK2 = "TokenOk2";
            final String SONG_OK = "SongOk";
            final String SONG_OK_2 = "SongOk2";
            final String SONG_KO = "SongKo";
            final String SONG_OK_3 = "SongOk3";
            final String SONG_PATH = "/v1/tracks/2TpxZ7JUBn3uw46aR7qd6V";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            String httpBaseUrl = wmRuntimeInfo.getHttpBaseUrl();
            Map<String, String> settings = Maps.newHashMap();
            settings.put(CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE, "true");
            settings.put(CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL, httpBaseUrl + "/.well-known/openid-configuration");
            settings.put(CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID, "44d34a4d05344c97837d463207805f8b");
            settings.put(CONFIG_DEFAULT_HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET, "3fc0576720544ac293a3a5304e6c0fa8");
            settings.put(HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE, "1");


            //init sinkRecord
            List<SinkRecord> records = Lists.newArrayList();
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), SONG_PATH, "GET", null),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord1);
            SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), SONG_PATH, "GET", null),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord2);
            SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), SONG_PATH, "GET", null),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord3);

            //define the http Mock Server interaction
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            Path path = Paths.get("src/test/resources/oauth2/wellknownUrlContent.json");
            httpBaseUrl = wmRuntimeInfo.getHttpBaseUrl();
            String content = Files.readString(path);
            String wellKnownUrlContent = content.replaceAll("baseUrl", httpBaseUrl);
            String scenario = "test_oauth2_authentication_client_credentials_flow_with_basic_auth_to_get_token";
            //good well known content
            wireMock
                    .register(WireMock.get(WELL_KNOWN_OPENID_CONFIGURATION)
                            .inScenario(scenario)
                            .whenScenarioStateIs(STARTED)
                            .willReturn(WireMock.aResponse()
                                    .withStatus(200)
                                    .withStatusMessage(OK)
                                    .withBody(wellKnownUrlContent)
                            ).willSetStateTo(WELL_KNOWN_OK)
                    );
            Path tokenPath = Paths.get("src/test/resources/oauth2/token.json");
            String tokenContent = Files.readString(tokenPath);

            wireMock
                    .register(
                            WireMock.post("/api/token")
                                    .inScenario(scenario)
                                    .withHeader("Content-Type", containing("application/x-www-form-urlencoded; charset=UTF-8"))
                                    .withHeader("Authorization", containing("Basic NDRkMzRhNGQwNTM0NGM5NzgzN2Q0NjMyMDc4MDVmOGI6M2ZjMDU3NjcyMDU0NGFjMjkzYTNhNTMwNGU2YzBmYTg="))
                                    .whenScenarioStateIs(UNAUTHORIZED)
                                    .willReturn(WireMock.aResponse()
                                            .withStatus(200)
                                            .withStatusMessage(OK)
                                            .withBody(tokenContent)
                                    ).willSetStateTo(TOKEN_OK)
                    );

            Path tokenPath2 = Paths.get("src/test/resources/oauth2/token2.json");
            String tokenContent2 = Files.readString(tokenPath2);

            wireMock
                    .register(
                            WireMock.post("/api/token")
                                    .inScenario(scenario)
                                    .withHeader("Content-Type", containing("application/x-www-form-urlencoded; charset=UTF-8"))
                                    .withHeader("Authorization", containing("Basic NDRkMzRhNGQwNTM0NGM5NzgzN2Q0NjMyMDc4MDVmOGI6M2ZjMDU3NjcyMDU0NGFjMjkzYTNhNTMwNGU2YzBmYTg="))
                                    .whenScenarioStateIs(SONG_KO)
                                    .willReturn(WireMock.aResponse()
                                            .withStatus(200)
                                            .withStatusMessage(OK)
                                            .withBody(tokenContent2)
                                    ).willSetStateTo(TOKEN_OK2)
                    );

            Path songPath = Paths.get("src/test/resources/oauth2/song.json");
            String songContent = Files.readString(songPath);
            wireMock
                    .register(
                            WireMock.get(SONG_PATH)
                                    .inScenario(scenario)
                                    .withHeader("Authorization", containing(BEARER_TOKEN))
                                    .whenScenarioStateIs(TOKEN_OK)
                                    .willReturn(WireMock.aResponse()
                                            .withStatus(200)
                                            .withStatusMessage(OK)
                                            .withBody(songContent)
                                    ).willSetStateTo(SONG_OK)
                    );
            wireMock
                    .register(
                            WireMock.get(SONG_PATH)
                                    .inScenario(scenario)
                                    .withHeader("Authorization", containing(BEARER_TOKEN))
                                    .whenScenarioStateIs(SONG_OK)
                                    .willReturn(WireMock.aResponse()
                                            .withStatus(200)
                                            .withStatusMessage(OK)
                                            .withBody(songContent)
                                    ).willSetStateTo(SONG_OK_2)
                    );
            wireMock
                    .register(
                            WireMock.get(SONG_PATH)
                                    .inScenario(scenario)
                                    .withHeader("Authorization", containing(BEARER_TOKEN))
                                    .whenScenarioStateIs(SONG_OK_2)
                                    .willReturn(WireMock.aResponse()
                                            .withStatus(401)
                                            .withStatusMessage(UNAUTHORIZED)
                                            .withHeader("WWW-Authenticate", "Bearer")
                                    ).willSetStateTo(SONG_KO)
                    );
            wireMock
                    .register(
                            WireMock.get(SONG_PATH)
                                    .inScenario(scenario)
                                    .withHeader("Authorization", containing(BEARER_TOKEN_2))
                                    .whenScenarioStateIs(TOKEN_OK2)
                                    .willReturn(WireMock.aResponse()
                                            .withStatus(200)
                                            .withStatusMessage(OK)
                                            .withBody(songContent)
                                    ).willSetStateTo(SONG_OK_3)
                    );
            wireMock
                    .register(
                            WireMock.get(SONG_PATH)
                                    .inScenario(scenario)
                                    .whenScenarioStateIs(WELL_KNOWN_OK)
                                    .willReturn(WireMock.aResponse()
                                            .withStatus(401)
                                            .withStatusMessage("Unauthorized") //401 + 'WWW-Authenticate' trigger challenge
                                            .withHeader("WWW-Authenticate", "Bearer")
                                            .withBody(songContent)
                                    ).willSetStateTo(UNAUTHORIZED)
                    );
            wireMock
                    .register(
                            WireMock.get(SONG_PATH)
                                    .inScenario(scenario)
                                    .whenScenarioStateIs(UNAUTHORIZED)
                                    .willReturn(WireMock.aResponse()
                                            .withStatus(401)
                                            .withStatusMessage("Unauthorized") //401 + 'WWW-Authenticate' trigger challenge
                                            .withHeader("WWW-Authenticate", "Bearer")
                                            .withBody(songContent)
                                    ).willSetStateTo(UNAUTHORIZED)
                    );

            //when
            okHttpSinkTask.start(settings);
            okHttpSinkTask.put(records);
            //then
            wireMock.verifyThat(3, getRequestedFor(urlEqualTo(SONG_PATH)).withHeader("Authorization", equalTo(BEARER_TOKEN)));
            wireMock.verifyThat(1, getRequestedFor(urlEqualTo(SONG_PATH)).withHeader("Authorization", equalTo(BEARER_TOKEN_2)));

        }

    }


    private HttpExchange getHttpExchange() {
        return getHttpExchange("http://www.titi.com", DUMMY_METHOD, 200);
    }

    private HttpExchange getHttpExchange(String url, HttpRequest.Method method, int statusCode) {
        Map<String, List<String>> requestHeaders = Maps.newHashMap();
        requestHeaders.put("X-dummy", Lists.newArrayList("blabla"));
        HttpRequest httpRequest = new HttpRequest(url, method);
        httpRequest.setHeaders(requestHeaders);
        httpRequest.setBodyAsString("stuff");
        HttpResponse httpResponse = new HttpResponse(statusCode, OK);
        httpResponse.setBodyAsString("my response");
        Map<String, List<String>> responseHeaders = Maps.newHashMap();
        responseHeaders.put("Content-Type", Lists.newArrayList("application/json"));
        httpResponse.setHeaders(responseHeaders);
        return new HttpExchange(
                httpRequest,
                httpResponse,
                245L,
                OffsetDateTime.now(ZoneId.of("UTC")),
                new AtomicInteger(1),
                true
        );
    }


    private String getLocalHttpRequestAsStringWithPath(int port, String path, String method, String dummyBody) {
        return "{\n" +
                "  \"url\": \"" + "http://localhost:" + port + path + "\",\n" +
                "  \"headers\": {},\n" +
                "  \"method\": \"" + method + "\",\n" +
                "  \"bodyAsString\": \"" + dummyBody + "\",\n" +
                "  \"bodyType\": \"" + DUMMY_BODY_TYPE + "\"\n" +
                "}";
    }


    private String getDummyHttpRequestAsString() {
        return "{\n" +
                "  \"url\": \"" + DUMMY_URL + "\",\n" +
                "  \"headers\": {},\n" +
                "  \"method\": \"" + DUMMY_METHOD + "\",\n" +
                "  \"bodyAsString\": \"" + DUMMY_BODY + "\",\n" +
                "  \"bodyType\": \"" + DUMMY_BODY_TYPE + "\"\n" +
                "}";
    }


    private String getHttpRequestAsString(String url,HttpRequest.Method method) {
        return "{\n" +
                "  \"url\": \"" + url + "\",\n" +
                "  \"headers\": {},\n" +
                "  \"method\": \"" + method + "\",\n" +
                "  \"bodyAsString\": \"" + DUMMY_BODY + "\",\n" +
                "  \"bodyType\": \"" + DUMMY_BODY_TYPE + "\"\n" +
                "}";
    }

    private int getRandomPort() {
        Random random = new Random();
        int low = 49152;
        int high = 65535;
        return random.nextInt(high - low) + low;
    }

    @NotNull
    private static HttpRequest getDummyHttpRequest(String url) {
        HttpRequest httpRequest = new HttpRequest(url, DUMMY_METHOD);
        Map<String, List<String>> headers = Maps.newHashMap();
        headers.put("Content-Type", Lists.newArrayList("application/json"));
        httpRequest.setHeaders(headers);
        httpRequest.setBodyAsString("stuff");
        httpRequest.setBodyAsForm(Maps.newHashMap());
        return httpRequest;
    }
}