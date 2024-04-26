package io.github.clescot.kafka.connect.http.sink;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.HttpTask;
import io.github.clescot.kafka.connect.http.client.ahc.AHCHttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.client.ssl.AlwaysTrustManagerFactory;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.core.queue.ConfigConstants;
import io.github.clescot.kafka.connect.http.core.queue.KafkaRecord;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.TrustManagerFactory;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class HttpSinkTaskTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSinkTaskTest.class);
    private static final String DUMMY_BODY = "stuff";
    private static final String DUMMY_URL = "http://www." + DUMMY_BODY + ".com";
    private static final String DUMMY_METHOD = "POST";
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


    @RegisterExtension
    static WireMockExtension wmHttp = WireMockExtension.newInstance()
            .options(
                    WireMockConfiguration.wireMockConfig()
                            .dynamicPort()
            )
            .build();


    @BeforeEach
    public void setUp() {
        QueueFactory.clearRegistrations();
        MockitoAnnotations.openMocks(this);
        httpSinkTask.initialize(sinkTaskContext);
    }

    @AfterEach
    public void tearsDown() {
        wmHttp.resetAll();
        HttpTask.removeCompositeMeterRegistry();
    }

    @AfterAll
    public static void shutdown() {

//        Awaitility.await()
//                .timeout(660, SECONDS)
//                .pollDelay(650, SECONDS)
//                .untilAsserted(() -> Assertions.assertTrue(true));
    }



    @Nested
    class Start {

        @Test
        void test_start_with_queue_name() {
            Assertions.assertDoesNotThrow(() -> {
                Map<String, String> settings = Maps.newHashMap();
                settings.put(ConfigConstants.QUEUE_NAME, "dummyQueueName");
                httpSinkTask.start(settings);
            });
        }

        @Test
        void test_start_with_custom_trust_store_path_and_password() {
            Assertions.assertDoesNotThrow(() -> {
                Map<String, String> settings = Maps.newHashMap();
                String truststorePath = Thread.currentThread().getContextClassLoader().getResource(CLIENT_TRUSTSTORE_JKS_FILENAME).getPath();
                settings.put(HTTP_CLIENT_SSL_TRUSTSTORE_PATH, truststorePath);
                settings.put(HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD, CLIENT_TRUSTSTORE_JKS_PASSWORD);
                httpSinkTask.start(settings);
            });
        }

        @Test
        void test_start_with_custom_trust_store_path_password_and_type() {
            Assertions.assertDoesNotThrow(() -> {
                Map<String, String> settings = Maps.newHashMap();
                String truststorePath = Thread.currentThread().getContextClassLoader().getResource(CLIENT_TRUSTSTORE_JKS_FILENAME).getPath();
                settings.put(HTTP_CLIENT_SSL_TRUSTSTORE_PATH, truststorePath);
                settings.put(HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD, CLIENT_TRUSTSTORE_JKS_PASSWORD);
                settings.put(HTTP_CLIENT_SSL_TRUSTSTORE_TYPE, JKS_STORE_TYPE);
                httpSinkTask.start(settings);
            });

        }

        @Test
        void test_start_with_custom_trust_store_path_password_type_and_algorithm() {
            Assertions.assertDoesNotThrow(() -> {
                Map<String, String> settings = Maps.newHashMap();
                String truststorePath = Thread.currentThread().getContextClassLoader().getResource(CLIENT_TRUSTSTORE_JKS_FILENAME).getPath();
                settings.put(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PATH, truststorePath);
                settings.put(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD, CLIENT_TRUSTSTORE_JKS_PASSWORD);
                settings.put(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_TYPE, JKS_STORE_TYPE);
                settings.put(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM, TRUSTSTORE_PKIX_ALGORITHM);
                httpSinkTask.start(settings);
            });
        }

        @Test
        void test_ssl_always_granted_parameter() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST, "true");
            httpSinkTask.start(settings);
            TrustManagerFactory trustManagerFactory = httpSinkTask.getHttpTask().getDefaultConfiguration().getHttpClient().getTrustManagerFactory();
            assertThat(trustManagerFactory).isInstanceOf(AlwaysTrustManagerFactory.class);
        }

        @Test
        void test_start_with_static_request_headers() {
            Assertions.assertDoesNotThrow(() -> {
                Map<String, String> settings = Maps.newHashMap();
                settings.put(STATIC_REQUEST_HEADER_NAMES, "param1,param2");
                settings.put("param1", "value1");
                settings.put("param2", "value2");
                httpSinkTask.start(settings);
            });
        }

        @Test
        void test_start_with_static_request_headers_without_required_parameters() {
            HttpSinkTask wsSinkTask = new HttpSinkTask();
            Map<String, String> settings = Maps.newHashMap();
            settings.put(STATIC_REQUEST_HEADER_NAMES, "param1,param2");
            Assertions.assertThrows(NullPointerException.class, () -> {
                wsSinkTask.start(settings);
            });

        }


        @Test
        void test_start_no_settings() {
            Assertions.assertDoesNotThrow(() -> {
                httpSinkTask.start(Maps.newHashMap());
            });
        }

        @Test
        void test_meter_registry_activate_jmx() {
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                settings.put(METER_REGISTRY_EXPORTER_JMX_ACTIVATE, "true");
                httpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path1"),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path2"),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path3"),
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
                                        .withStatusMessage("OK")
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/path2")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage("OK")
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/path3")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage("OK")
                                        .withFixedDelay(1000)
                                )
                        );
                //when

                httpSinkTask.put(records);


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
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                settings.put(METER_REGISTRY_EXPORTER_PROMETHEUS_ACTIVATE, "true");
                settings.put(METER_REGISTRY_EXPORTER_PROMETHEUS_PORT, "9090");
                httpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path1"),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path2"),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path3"),
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
                                        .withStatusMessage("OK")
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/path2")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage("OK")
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/path3")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage("OK")
                                        .withFixedDelay(1000)
                                )
                        );
                //when

                httpSinkTask.put(records);


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
                httpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path1"),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path2"),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path3"),
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
                                        .withStatusMessage("OK")
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/path2")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage("OK")
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/path3")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage("OK")
                                        .withFixedDelay(1000)
                                )
                        );
                //when

                httpSinkTask.put(records);


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
            Assertions.assertDoesNotThrow(() -> {
                HashMap<String, String> settings = Maps.newHashMap();
                settings.put(METER_REGISTRY_EXPORTER_JMX_ACTIVATE, "true");
                settings.put(METER_REGISTRY_EXPORTER_PROMETHEUS_ACTIVATE, "true");
                settings.put(METER_REGISTRY_EXPORTER_PROMETHEUS_PORT, "9090");
                settings.put(METER_REGISTRY_BIND_METRICS_EXECUTOR_SERVICE, "true");
                settings.put(METER_REGISTRY_BIND_METRICS_JVM_MEMORY, "true");
                settings.put(METER_REGISTRY_BIND_METRICS_JVM_THREAD, "true");
                settings.put(METER_REGISTRY_BIND_METRICS_JVM_INFO, "true");
                settings.put(METER_REGISTRY_BIND_METRICS_JVM_GC, "true");
                settings.put(METER_REGISTRY_BIND_METRICS_JVM_CLASSLOADER, "true");
                settings.put(METER_REGISTRY_BIND_METRICS_JVM_PROCESSOR, "true");
                settings.put(METER_REGISTRY_BIND_METRICS_LOGBACK, "true");
                httpSinkTask.start(settings);

                //given
                WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();


                //init sinkRecord
                List<SinkRecord> records = Lists.newArrayList();
                List<Header> headers = Lists.newArrayList();
                SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path1"),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord1);
                SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path2"),
                        -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
                records.add(sinkRecord2);
                SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                        getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path3"),
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
                                        .withStatusMessage("OK")
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/path2")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage("OK")
                                        .withFixedDelay(1000)
                                )
                        );
                wireMock
                        .register(WireMock.post("/path3")
                                .willReturn(WireMock.aResponse()
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(bodyResponse)
                                        .withStatus(200)
                                        .withStatusMessage("OK")
                                        .withFixedDelay(1000)
                                )
                        );
                //when

                httpSinkTask.put(records);


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
    class Stop {
        @Test
        void test_stop_with_start_and_no_setttings() {
            httpSinkTask.start(Maps.newHashMap());
            Assertions.assertDoesNotThrow(() -> httpSinkTask.stop());
        }

        @Test
        void test_stop_without_start() {
            Assertions.assertDoesNotThrow(() -> httpSinkTask.stop());
        }

    }

    @Nested
    class Put {
        @Test
        void test_put_add_static_headers_with_value_as_string() {
            //given
            Map<String, String> settings = Maps.newHashMap();
            settings.put(DEFAULT_CONFIGURATION_PREFIX + STATIC_REQUEST_HEADER_NAMES, "param1,param2");
            settings.put(DEFAULT_CONFIGURATION_PREFIX + STATIC_REQUEST_HEADER_PREFIX + "param1", "value1");
            settings.put(DEFAULT_CONFIGURATION_PREFIX + STATIC_REQUEST_HEADER_PREFIX + "param2", "value2");
            httpSinkTask.start(settings);
            OkHttpClient httpClient = Mockito.mock(OkHttpClient.class);
            HttpExchange dummyHttpExchange = getDummyHttpExchange();
            when(httpClient.call(any(HttpRequest.class), any(AtomicInteger.class))).thenReturn(CompletableFuture.supplyAsync(() -> dummyHttpExchange));
            httpSinkTask.getDefaultConfiguration().setHttpClient(httpClient);
            List<SinkRecord> records = Lists.newArrayList();
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, getDummyHttpRequestAsString(), -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord);
            //when
            httpSinkTask.put(records);
            ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
            verify(httpClient, times(1)).call(captor.capture(), any(AtomicInteger.class));
            HttpRequest enhancedRecordBeforeHttpCall = captor.getValue();
            //then
            assertThat(enhancedRecordBeforeHttpCall.getHeaders()).hasSize(sinkRecord.headers().size() + httpSinkTask.getDefaultConfiguration().getAddStaticHeadersFunction().getStaticHeaders().size());
            assertThat(enhancedRecordBeforeHttpCall.getHeaders()).contains(Map.entry("param1", Lists.newArrayList("value1")));
            assertThat(enhancedRecordBeforeHttpCall.getHeaders()).contains(Map.entry("param2", Lists.newArrayList("value2")));
        }

        @Test
        void test_put_nominal_case_with_value_as_string() {
            //given
            Map<String, String> settings = Maps.newHashMap();
            httpSinkTask.start(settings);

            //mock httpClient
            AHCHttpClient httpClient = Mockito.mock(AHCHttpClient.class);
            HttpExchange dummyHttpExchange = getDummyHttpExchange();
            when(httpClient.call(any(HttpRequest.class), any(AtomicInteger.class))).thenReturn(CompletableFuture.supplyAsync(() -> dummyHttpExchange));
            httpSinkTask.getDefaultConfiguration().setHttpClient(httpClient);

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
            httpSinkTask.start(settings);

            //mock httpClient
            AHCHttpClient httpClient = Mockito.mock(AHCHttpClient.class);
            HttpExchange dummyHttpExchange = getDummyHttpExchange();
            when(httpClient.call(any(HttpRequest.class), any(AtomicInteger.class))).thenReturn(CompletableFuture.supplyAsync(() -> dummyHttpExchange));
            httpSinkTask.getDefaultConfiguration().setHttpClient(httpClient);

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
            httpSinkTask.start(settings);

            //mock httpClient
            AHCHttpClient httpClient = Mockito.mock(AHCHttpClient.class);
            HttpExchange dummyHttpExchange = getDummyHttpExchange();
            when(httpClient.call(any(HttpRequest.class), any(AtomicInteger.class))).thenReturn(CompletableFuture.supplyAsync(() -> dummyHttpExchange));
            httpSinkTask.getDefaultConfiguration().setHttpClient(httpClient);

            //init sinkRecord
            List<SinkRecord> records = Lists.newArrayList();
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, null, -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord);

            //when
            Assertions.assertThrows(RuntimeException.class, () -> httpSinkTask.put(records));

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
                    () -> httpSinkTask.start(settings));

        }


        @Test
        void test_put_with_publish_in_memory_set_to_false() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put(PUBLISH_MODE, PublishMode.NONE.name());
            httpSinkTask.start(settings);
            AHCHttpClient httpClient = Mockito.mock(AHCHttpClient.class);
            HttpExchange dummyHttpExchange = getDummyHttpExchange();
            when(httpClient.call(any(HttpRequest.class), any(AtomicInteger.class))).thenReturn(CompletableFuture.supplyAsync(() -> dummyHttpExchange));
            httpSinkTask.getDefaultConfiguration().setHttpClient(httpClient);
            Queue<KafkaRecord> queue = mock(Queue.class);
            httpSinkTask.setQueue(queue);
            List<SinkRecord> records = Lists.newArrayList();
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, getDummyHttpRequestAsString(), -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord);
            httpSinkTask.put(records);
            verify(httpClient, times(1)).call(any(HttpRequest.class), any(AtomicInteger.class));
            verify(queue, never()).offer(any(KafkaRecord.class));
        }

        @Test
        void test_put_with_publish_to_in_memory_queue_set_to_true_with_a_consumer() {

            //given
            Map<String, String> settings = Maps.newHashMap();
            settings.put(PUBLISH_MODE, PublishMode.IN_MEMORY_QUEUE.name());
            QueueFactory.registerConsumerForQueue(QueueFactory.DEFAULT_QUEUE_NAME);
            httpSinkTask.start(settings);
            AHCHttpClient httpClient = Mockito.mock(AHCHttpClient.class);
            HttpExchange dummyHttpExchange = getDummyHttpExchange();
            when(httpClient.call(any(HttpRequest.class), any(AtomicInteger.class))).thenReturn(CompletableFuture.supplyAsync(() -> dummyHttpExchange));
            httpSinkTask.getDefaultConfiguration().setHttpClient(httpClient);
            Queue<KafkaRecord> queue = mock(Queue.class);
            httpSinkTask.setQueue(queue);
            List<SinkRecord> records = Lists.newArrayList();
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, getDummyHttpRequestAsString(), -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord);
            //when
            httpSinkTask.put(records);

            //then
            verify(httpClient, times(1)).call(any(HttpRequest.class), any(AtomicInteger.class));
            verify(queue, times(1)).offer(any(KafkaRecord.class));
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
            httpSinkTask.start(settings);


            //init sinkRecord
            List<SinkRecord> records = Lists.newArrayList();
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path1"),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord1);
            SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path2"),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord2);
            SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path3"),
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
                                    .withStatusMessage("OK")
                                    .withFixedDelay(1000)
                            )
                    );
            wireMock
                    .register(WireMock.post("/path2")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                                    .withFixedDelay(1000)
                            )
                    );
            wireMock
                    .register(WireMock.post("/path3")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                                    .withFixedDelay(1000)
                            )
                    );
            //when
            Stopwatch stopwatch = Stopwatch.createStarted();
            httpSinkTask.put(records);
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
            httpSinkTask.start(settings);


            //init sinkRecord
            List<SinkRecord> records = Lists.newArrayList();
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path1"),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord1);
            SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path2"),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord2);
            SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path3"),
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
                                    .withStatusMessage("OK")
                                    .withFixedDelay(1000)
                            )
                    );
            wireMock
                    .register(WireMock.post("/path2")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                                    .withFixedDelay(1000)
                            )
                    );
            wireMock
                    .register(WireMock.post("/path3")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                                    .withFixedDelay(1000)
                            )
                    );
            //when
            Stopwatch stopwatch = Stopwatch.createStarted();

            httpSinkTask.put(records);
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

            httpSinkTask.start(settings);


            //init sinkRecord
            List<SinkRecord> records = Lists.newArrayList();
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path1"),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord1);
            SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path2"),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord2);
            SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path3"),
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
                                    .withStatusMessage("OK")
                                    .withFixedDelay(1000)
                            )
                    );
            wireMock
                    .register(WireMock.post("/path2")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                                    .withFixedDelay(1000)
                            )
                    );
            wireMock
                    .register(WireMock.post("/path3")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                                    .withFixedDelay(1000)
                            )
                    );
            //when
            Stopwatch stopwatch = Stopwatch.createStarted();
            httpSinkTask.put(records);
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
            httpSinkTask.start(settings);


            //init sinkRecord
            List<SinkRecord> records = Lists.newArrayList();
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord1 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path1"),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord1);
            SinkRecord sinkRecord2 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path2"),
                    -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            records.add(sinkRecord2);
            SinkRecord sinkRecord3 = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA,
                    getLocalHttpRequestAsStringWithPath(wmRuntimeInfo.getHttpPort(), "/path3"),
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
                                    .withStatusMessage("OK")
                                    .withFixedDelay(1000)
                            )
                    );
            wireMock
                    .register(WireMock.post("/path2")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                                    .withFixedDelay(1000)
                            )
                    );
            wireMock
                    .register(WireMock.post("/path3")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                                    .withFixedDelay(1000)
                            )
                    );
            //when
            Stopwatch stopwatch = Stopwatch.createStarted();

            httpSinkTask.put(records);
            stopwatch.stop();
            long elapsedMillis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            LOGGER.info("put method execution time :'{}' ms", elapsedMillis);
            //then
            assertThat(elapsedMillis).isLessThan(2950);

        }


    }



    private HttpExchange getDummyHttpExchange() {
        Map<String, List<String>> requestHeaders = Maps.newHashMap();
        requestHeaders.put("X-dummy", Lists.newArrayList("blabla"));
        HttpRequest httpRequest = new HttpRequest("http://www.titi.com", DUMMY_METHOD, DUMMY_BODY_TYPE);
        httpRequest.setHeaders(requestHeaders);
        httpRequest.setBodyAsString("stuff");
        HttpResponse httpResponse = new HttpResponse(200, "OK");
        httpResponse.setResponseBody("my response");
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


    private String getLocalHttpRequestAsStringWithPath(int port, String path) {
        return "{\n" +
                "  \"url\": \"" + "http://localhost:" + port + path + "\",\n" +
                "  \"headers\": {},\n" +
                "  \"method\": \"POST\",\n" +
                "  \"bodyAsString\": \"" + DUMMY_BODY + "\",\n" +
                "  \"bodyAsByteArray\": [],\n" +
                "  \"bodyAsForm\": {},\n" +
                "  \"bodyAsMultipart\": [],\n" +
                "  \"bodyType\": \"" + DUMMY_BODY_TYPE + "\"\n" +
                "}";
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



}