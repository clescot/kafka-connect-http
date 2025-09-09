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
import io.github.clescot.kafka.connect.http.HttpTask;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.sink.HttpConnectorConfig;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.common.config.AbstractConfig;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static io.github.clescot.kafka.connect.Configuration.DEFAULT_CONFIGURATION_ID;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.DEFAULT_DEFAULT_RETRY_RESPONSE_CODE_REGEX;
import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD)
class HttpConfigurationTest {
    private static final HttpRequest.Method DUMMY_METHOD = HttpRequest.Method.POST;

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
    class CallWithRetryPolicy {
        ExecutorService executorService;
        OkHttpClient okHttpClient;
        @BeforeEach
        void setUp(){
            Map<String,String> configs = Maps.newHashMap();
            AbstractConfig config = new HttpConnectorConfig(configs);

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




}