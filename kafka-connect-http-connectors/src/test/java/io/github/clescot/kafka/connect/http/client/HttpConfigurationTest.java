package io.github.clescot.kafka.connect.http.client;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.trafficlistener.ConsoleNotifyingWiremockNetworkTrafficListener;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dev.failsafe.RetryPolicy;
import io.github.clescot.kafka.connect.MapUtils;
import io.github.clescot.kafka.connect.VersionUtils;
import io.github.clescot.kafka.connect.http.HttpTask;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.sink.HttpConnectorConfig;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.common.config.AbstractConfig;
import org.assertj.core.util.Sets;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static io.github.clescot.kafka.connect.Configuration.DEFAULT_CONFIGURATION_ID;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.*;
import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD)
class HttpConfigurationTest {
    private static final HttpRequest.Method DUMMY_METHOD = HttpRequest.Method.POST;

    public static final String AUTHORIZED_STATE = "Authorized";
    public static final String INTERNAL_SERVER_ERROR_STATE = "InternalServerError";
    private static final String DUMMY_BODY = "stuff";
    private static final String DUMMY_URL = "http://www." + DUMMY_BODY + ".com";
    OkHttpClient okHttpClient;


    private final ExecutorService executorService = Executors.newFixedThreadPool(2);
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

    @BeforeEach
    void setup() {
        OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
        Map<String, String> settings = Map.of("url", "http://example.com/sse", "topic", "test-topic");
        okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
    }

    @Nested
    class CallWithRetryPolicy {
        ExecutorService executorService;
        OkHttpClient okHttpClient;
        @BeforeEach
        void setUp(){
            Map<String,String> configs = Maps.newHashMap();

            CompositeMeterRegistry compositeMeterRegistry = new CompositeMeterRegistry();
            JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
            jmxMeterRegistry.start();
            compositeMeterRegistry.add(jmxMeterRegistry);
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Map.of("url", "http://example.com/sse", "topic", "test-topic");
            okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            executorService = Executors.newFixedThreadPool(2);
            HttpConfiguration<OkHttpClient, Request, Response> httpConfiguration = new HttpConfiguration<>("test",okHttpClient,executorService, null,settings);
            Map<String, HttpConfiguration<OkHttpClient, Request, Response>> map = Maps.newHashMap();
            map.put(DEFAULT_CONFIGURATION_ID, httpConfiguration);
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
            String dummy = "dummy";
            HttpConfiguration<OkHttpClient,Request, Response> httpConfiguration = new HttpConfiguration<>(dummy,okHttpClient,executorService,null, settings);
            HttpExchange httpExchange = httpConfiguration.call(httpRequest).get();

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
            settings.put("retry.policy.retries","2");
            settings.put("retry.policy.response.code.regex",DEFAULT_DEFAULT_RETRY_RESPONSE_CODE_REGEX);
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(settings);
            HttpTask httpTask = new HttpTask(httpConnectorConfig,new OkHttpClientFactory());

            RetryPolicy<HttpExchange> retryPolicy = httpTask.buildRetryPolicy(httpConnectorConfig.originalsStrings());
            String dummy = "dummy";
            HttpConfiguration<OkHttpClient,okhttp3.Request,okhttp3.Response> httpConfiguration = new HttpConfiguration<>(dummy,okHttpClient,executorService, retryPolicy,settings);
            HttpExchange httpExchange = httpConfiguration.call(httpRequest).get();

            //then
            AtomicInteger attempts = httpExchange.getAttempts();
            assertThat(attempts.get()).isEqualTo(2);
            assertThat(httpExchange.isSuccess()).isTrue();
        }

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

    @Nested
    class TestEnrichHttpExchange {


        @Test
        void test_is_success_with_200() {

            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.dummy." + SUCCESS_RESPONSE_CODE_REGEX, "^2[0-9][0-9]$");
            String configId = "dummy";
            HttpExchange httpExchange = getDummyHttpExchange();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(configId,okHttpClient, executorService, null,settings);
            boolean success = httpConfiguration.enrichHttpExchange(httpExchange).isSuccess();
            assertThat(success).isTrue();
        }

        @Test
        void test_is_not_success_with_200_by_configuration() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put(SUCCESS_RESPONSE_CODE_REGEX, "^1[0-9][0-9]$");
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            String configId = "dummy";
            settings.put("configuration.id", configId);
            okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            HttpExchange httpExchange = getDummyHttpExchange();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(configId,okHttpClient, executorService, null,settings);
            boolean success = httpConfiguration.enrichHttpExchange(httpExchange).isSuccess();
            assertThat(success).isFalse();
        }

    }

    @Nested
    class AddSuccessStatusToHttpExchangeFunction {
        private final HttpRequest.Method dummyMethod = HttpRequest.Method.POST;

        private final ExecutorService executorService = Executors.newFixedThreadPool(2);

        @Test
        void test_is_success_with_200() {

            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.dummy." + SUCCESS_RESPONSE_CODE_REGEX, "^2[0-9][0-9]$");
            String configId = "dummy";
            HttpExchange httpExchange = getDummyHttpExchange();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(configId,okHttpClient, executorService, null,settings);
            boolean success = httpConfiguration.enrichHttpExchange(httpExchange).isSuccess();
            assertThat(success).isTrue();
        }

        @NotNull
        private CompositeMeterRegistry getCompositeMeterRegistry() {
            JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry(s -> null, Clock.SYSTEM);
            HashSet<MeterRegistry> registries = Sets.newHashSet();
            registries.add(jmxMeterRegistry);
            return new CompositeMeterRegistry(Clock.SYSTEM, registries);
        }

        @Test
        void test_is_not_success_with_200_by_configuration() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put(SUCCESS_RESPONSE_CODE_REGEX, "^1[0-9][0-9]$");
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            String configId = "dummy";
            settings.put("configuration.id", configId);
            okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            HttpExchange httpExchange = getDummyHttpExchange();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(configId,okHttpClient, executorService, null,settings);
            boolean success = httpConfiguration.enrichHttpExchange(httpExchange).isSuccess();
            assertThat(success).isFalse();
        }


        private HttpExchange getDummyHttpExchange() {
            Map<String, List<String>> requestHeaders = Maps.newHashMap();
            requestHeaders.put("X-dummy", Lists.newArrayList("blabla"));
            HttpRequest httpRequest = new HttpRequest("http://www.titi.com", dummyMethod);
            httpRequest.setHeaders(requestHeaders);
            httpRequest.setBodyAsString("stuff");
            HttpResponse httpResponse = new HttpResponse(200, "OK");
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
    }

    @Nested
    class RetryNeeded {
        @Test
        void test_retry_needed() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.dummy." + RETRY_RESPONSE_CODE_REGEX, "^5[0-9][0-9]$");
            String configId = "dummy";
            HttpResponse httpResponse = new HttpResponse(500, "Internal Server Error");
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(configId,okHttpClient, executorService, null,settings);
            boolean retryNeeded = httpConfiguration.retryNeeded(httpResponse);
            assertThat(retryNeeded).isTrue();
        }

        @Test
        void test_retry_not_needed_with_400_status_code() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("httpclient.dummy." + RETRY_RESPONSE_CODE_REGEX, "^5[0-9][0-9]$");
            String configId = "dummy";
            HttpResponse httpResponse = new HttpResponse(400, "Internal Server Error");
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(configId,okHttpClient, executorService, null,settings);
            boolean retryNeeded = httpConfiguration.retryNeeded(httpResponse);
            assertThat(retryNeeded).isFalse();
        }

        @Test
        void test_retry_not_needed_with_200_status_code() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("httpclient.dummy." + RETRY_RESPONSE_CODE_REGEX, "^5[0-9][0-9]$");
            String configId = "dummy";
            HttpResponse httpResponse = new HttpResponse(200, "Internal Server Error");
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(configId,okHttpClient, executorService,null, settings);
            boolean retryNeeded = httpConfiguration.retryNeeded(httpResponse);
            assertThat(retryNeeded).isFalse();
        }


        @Test
        void test_retry_needed_by_configuration_with_200_status_code() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.dummy." + RETRY_RESPONSE_CODE_REGEX, "^2[0-9][0-9]$");
            settings.put(CONFIG_DEFAULT_RETRY_RESPONSE_CODE_REGEX, "^[1-5][0-9][0-9]$");
            Map<String, String> configSettings = MapUtils.getMapWithPrefix(new HttpConnectorConfig(settings).originalsStrings(), "config.dummy.");
            String configId = "dummy";
            HttpResponse httpResponse = new HttpResponse(200, "Internal Server Error");
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(configId,okHttpClient, executorService, null,configSettings);
            boolean retryNeeded = httpConfiguration.retryNeeded(httpResponse);
            assertThat(retryNeeded).isTrue();
        }
    }

    @Nested
    class TestEnrichHttpRequest {
        private final ExecutorService executorService = Executors.newFixedThreadPool(2);
        private final VersionUtils versionUtils = new VersionUtils();


        @Test
        void test_add_static_headers() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put(STATIC_REQUEST_HEADER_NAMES, "X-Stuff-Id,X-Super-Option");
            settings.put(STATIC_REQUEST_HEADER_PREFIX + "X-Stuff-Id", "12345");
            settings.put(STATIC_REQUEST_HEADER_PREFIX + "X-Super-Option", "ABC");
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            String configId = "dummy";
            settings.put("configuration.id", configId);
            settings.put(USER_AGENT_OVERRIDE, "custom");
            settings.put(USER_AGENT_CUSTOM_VALUES, "custom_ua");
            okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());

            HttpRequest httpRequest = getDummyHttpRequest();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(configId,okHttpClient, executorService, null,settings);
            HttpRequest enrichedHttpRequest = httpConfiguration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers).containsEntry("X-Stuff-Id", Lists.newArrayList("12345"))
                    .containsEntry("X-Super-Option", Lists.newArrayList("ABC"));
        }

        @Test
        void test_generate_missing_request_id() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            String configId = "dummy";
            settings.put("configuration.id", configId);
            settings.put(USER_AGENT_OVERRIDE, "custom");
            settings.put(USER_AGENT_CUSTOM_VALUES, "custom_ua");
            settings.put(GENERATE_MISSING_REQUEST_ID, "true");
            okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(configId,okHttpClient, executorService, null,settings);
            HttpRequest enrichedHttpRequest = httpConfiguration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers).containsKey("X-Request-ID");
        }

        @Test
        void test_generate_missing_correlation_id() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            String configId = "dummy";
            settings.put("configuration.id", configId);
            settings.put(USER_AGENT_OVERRIDE, "custom");
            settings.put(USER_AGENT_CUSTOM_VALUES, "custom_ua");
            settings.put(GENERATE_MISSING_CORRELATION_ID, "true");
            okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(configId,okHttpClient, executorService, null,settings);
            HttpRequest enrichedHttpRequest = httpConfiguration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers).containsKey("X-Correlation-ID");
        }

        @Test
        @DisplayName("test override User-Agent header with 'custom' value")
        void test_activating_user_agent_interceptor_with_custom_value() {
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Map<String, String> settings = Maps.newHashMap();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            String configId = "dummy";
            settings.put("configuration.id", configId);
            settings.put(USER_AGENT_OVERRIDE, "custom");
            settings.put(USER_AGENT_CUSTOM_VALUES, "custom_ua");
            okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            //given
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(configId,okHttpClient, executorService, null,settings);
            HttpRequest enrichedHttpRequest = httpConfiguration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers).containsEntry("User-Agent", Lists.newArrayList("custom_ua"));
        }

        @Test
        @DisplayName("test override User-Agent header with multiple 'custom' value")
        void test_activating_user_agent_interceptor_with_multiple_custom_value() {

            //given
            Map<String, String> settings = Maps.newHashMap();
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            String configId = "dummy";
            settings.put("configuration.id", configId);
            settings.put(USER_AGENT_OVERRIDE, "custom");
            settings.put(USER_AGENT_CUSTOM_VALUES, "custom_1|custom_2|custom_3");
            okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());

            HttpRequest httpRequest = getDummyHttpRequest();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(configId,okHttpClient, executorService, null,settings);
            HttpRequest enrichedHttpRequest = httpConfiguration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers.get("User-Agent")).isSubsetOf(Lists.newArrayList("custom_1", "custom_2", "custom_3"));
        }

        @Test
        @DisplayName("test override User-Agent header with already 'User-Agent' defined in Http Request")
        void test_activating_user_agent_interceptor_with_already_defined_user_agent_in_http_request() {

            //given
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.dummy." + USER_AGENT_OVERRIDE, "custom");
            settings.put("config.dummy." + USER_AGENT_CUSTOM_VALUES, "custom_1|custom_2|custom_3");
            String configId = "dummy";
            HttpRequest httpRequest = getDummyHttpRequest();
            httpRequest.getHeaders().put("User-Agent", Lists.newArrayList("already"));
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(configId,okHttpClient, executorService, null,settings);
            HttpRequest enrichedHttpRequest = httpConfiguration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers.get("User-Agent").get(0)).isEqualTo("already");
        }

        @Test
        @DisplayName("test override User-Agent header with 'http_client' settings")
        void test_activating_user_agent_interceptor_with_http_client_scope() {

            //given
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.dummy." + USER_AGENT_OVERRIDE, "http_client");
            String configId = "dummy";
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(configId,okHttpClient, executorService, null,settings);
            HttpRequest enrichedHttpRequest = httpConfiguration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            //user-agent in this cas is set by the underlying http client implementation
            //after the enrichment phase in the configuration
            assertThat(headers.get("User-Agent")).isNull();
        }

        @Test
        @DisplayName("test override User-Agent header with 'project' value")
        void test_activating_user_agent_interceptor_with_project_value() {

            //given
            Map<String, String> settings = Maps.newHashMap();
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            settings.put("url", "http://example.com/sse");
            settings.put("topic", "test-topic");
            String configId = "dummy";
            settings.put("configuration.id", configId);
            settings.put(USER_AGENT_OVERRIDE, "project");
            okHttpClient = okHttpClientFactory.buildHttpClient(settings, null, new CompositeMeterRegistry(), new Random());
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(configId,okHttpClient, executorService, null,settings);
            HttpRequest enrichedHttpRequest = httpConfiguration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers).containsEntry("User-Agent", Lists.newArrayList("Mozilla/5.0 (compatible;kafka-connect-http/" + versionUtils.getVersion() + "; okhttp; https://github.com/clescot/kafka-connect-http)"));

        }

    }


    @NotNull
    private CompositeMeterRegistry getCompositeMeterRegistry() {
        JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry(s -> null, Clock.SYSTEM);
        HashSet<MeterRegistry> registries = Sets.newHashSet();
        registries.add(jmxMeterRegistry);
        return new CompositeMeterRegistry(Clock.SYSTEM, registries);
    }


    private HttpExchange getDummyHttpExchange() {
        HttpRequest httpRequest = getDummyHttpRequest();
        HttpResponse httpResponse = new HttpResponse(200, "OK");
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

    @NotNull
    private static HttpRequest getDummyHttpRequest() {
        HttpRequest httpRequest = new HttpRequest(DUMMY_URL, DUMMY_METHOD);
        Map<String, List<String>> headers = Maps.newHashMap();
        headers.put("Content-Type", Lists.newArrayList("application/json"));
        httpRequest.setHeaders(headers);
        httpRequest.setBodyAsString("stuff");
        httpRequest.setBodyAsForm(Maps.newHashMap());
        return httpRequest;
    }
}