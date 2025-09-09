package io.github.clescot.kafka.connect.http.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.MapUtils;
import io.github.clescot.kafka.connect.VersionUtils;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.sink.HttpConnectorConfig;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.jmx.JmxMeterRegistry;
import okhttp3.Request;
import okhttp3.Response;
import org.assertj.core.util.Sets;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.*;
import static org.assertj.core.api.Assertions.assertThat;

class HttpHttpClientConfigurationTest {

    private final ExecutorService executorService = Executors.newFixedThreadPool(2);
    private static final String DUMMY_BODY = "stuff";
    private static final String DUMMY_URL = "http://www." + DUMMY_BODY + ".com";
    private static final HttpRequest.Method DUMMY_METHOD = HttpRequest.Method.POST;

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


    @Nested
    class TestEnrichHttpRequest {
        private final ExecutorService executorService = Executors.newFixedThreadPool(2);
        private final VersionUtils versionUtils = new VersionUtils();

        @Test
        void test_add_static_headers() {
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + STATIC_REQUEST_HEADER_NAMES, "X-Stuff-Id,X-Super-Option");
            config.put("config.dummy." + STATIC_REQUEST_HEADER_PREFIX + "X-Stuff-Id", "12345");
            config.put("config.dummy." + STATIC_REQUEST_HEADER_PREFIX + "X-Super-Option", "ABC");
            HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration = new HttpClientConfiguration<>(
                    "dummy",
                    new OkHttpClientFactory(),
                    MapUtils.getMapWithPrefix(new HttpConnectorConfig(config).originalsStrings(), "config.dummy."),
                    executorService,
                    getCompositeMeterRegistry()
            );
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(httpClientConfiguration,executorService);
            HttpRequest enrichedHttpRequest = httpConfiguration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers).containsEntry("X-Stuff-Id", Lists.newArrayList("12345"))
                    .containsEntry("X-Super-Option", Lists.newArrayList("ABC"));
        }

        @Test
        void test_generate_missing_request_id() {
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + GENERATE_MISSING_REQUEST_ID, "true");
            HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration = new HttpClientConfiguration<>(
                    "dummy",
                    new OkHttpClientFactory(),
                    MapUtils.getMapWithPrefix(new HttpConnectorConfig(config).originalsStrings(), "config.dummy."),
                    executorService,
                    getCompositeMeterRegistry()
            );
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(httpClientConfiguration,executorService);
            HttpRequest enrichedHttpRequest = httpConfiguration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers).containsKey("X-Request-ID");
        }

        @Test
        void test_generate_missing_correlation_id() {
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + GENERATE_MISSING_CORRELATION_ID, "true");
            HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration = new HttpClientConfiguration<>("dummy",
                    new OkHttpClientFactory(),
                    MapUtils.getMapWithPrefix(new HttpConnectorConfig(config).originalsStrings(), "config.dummy."),
                    executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(httpClientConfiguration,executorService);
            HttpRequest enrichedHttpRequest = httpConfiguration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers).containsKey("X-Correlation-ID");
        }

        @Test
        @DisplayName("test override User-Agent header with 'custom' value")
        void test_activating_user_agent_interceptor_with_custom_value() {

            //given
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + USER_AGENT_OVERRIDE, "custom");
            config.put("config.dummy." + USER_AGENT_CUSTOM_VALUES, "custom_ua");
            HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration = new HttpClientConfiguration<>("dummy",
                    new OkHttpClientFactory(),
                    MapUtils.getMapWithPrefix(new HttpConnectorConfig(config).originalsStrings(), "config.dummy."),
                    executorService,
                    getCompositeMeterRegistry());
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(httpClientConfiguration,executorService);
            HttpRequest enrichedHttpRequest = httpConfiguration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers).containsEntry("User-Agent", Lists.newArrayList("custom_ua"));
        }

        @Test
        @DisplayName("test override User-Agent header with multiple 'custom' value")
        void test_activating_user_agent_interceptor_with_multiple_custom_value() {

            //given
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + USER_AGENT_OVERRIDE, "custom");
            config.put("config.dummy." + USER_AGENT_CUSTOM_VALUES, "custom_1|custom_2|custom_3");
            HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration = new HttpClientConfiguration<>("dummy",
                    new OkHttpClientFactory(),
                    MapUtils.getMapWithPrefix(new HttpConnectorConfig(config).originalsStrings(), "config.dummy."),
                    executorService,
                    getCompositeMeterRegistry());
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(httpClientConfiguration,executorService);
            HttpRequest enrichedHttpRequest = httpConfiguration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers.get("User-Agent")).isSubsetOf(Lists.newArrayList("custom_1", "custom_2", "custom_3"));
        }

        @Test
        @DisplayName("test override User-Agent header with already 'User-Agent' defined in Http Request")
        void test_activating_user_agent_interceptor_with_already_defined_user_agent_in_http_request() {

            //given
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + USER_AGENT_OVERRIDE, "custom");
            config.put("config.dummy." + USER_AGENT_CUSTOM_VALUES, "custom_1|custom_2|custom_3");
            HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration = new HttpClientConfiguration<>("dummy",
                    new OkHttpClientFactory(),
                    MapUtils.getMapWithPrefix(new HttpConnectorConfig(config).originalsStrings(), "config.dummy."),
                    executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest = getDummyHttpRequest();
            httpRequest.getHeaders().put("User-Agent", Lists.newArrayList("already"));
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(httpClientConfiguration,executorService);
            HttpRequest enrichedHttpRequest = httpConfiguration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers.get("User-Agent").get(0)).isEqualTo("already");
        }

        @Test
        @DisplayName("test override User-Agent header with 'http_client' settings")
        void test_activating_user_agent_interceptor_with_http_client_scope() {

            //given
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + USER_AGENT_OVERRIDE, "http_client");
            HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration = new HttpClientConfiguration<>("dummy",
                    new OkHttpClientFactory(), MapUtils.getMapWithPrefix(new HttpConnectorConfig(config).originalsStrings(), "config.dummy."),
                    executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(httpClientConfiguration,executorService);
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
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + USER_AGENT_OVERRIDE, "project");
            HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration = new HttpClientConfiguration<>("dummy",
                    new OkHttpClientFactory(), MapUtils.getMapWithPrefix(new HttpConnectorConfig(config).originalsStrings(), "config.dummy."),
                    executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(httpClientConfiguration,executorService);
            HttpRequest enrichedHttpRequest = httpConfiguration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers).containsEntry("User-Agent", Lists.newArrayList("Mozilla/5.0 (compatible;kafka-connect-http/" + versionUtils.getVersion() + "; okhttp; https://github.com/clescot/kafka-connect-http)"));

        }

    }

    @Nested
    class TestEnrichHttpExchange {

        private final ExecutorService executorService = Executors.newFixedThreadPool(2);

        @Test
        void test_is_success_with_200() {

            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + SUCCESS_RESPONSE_CODE_REGEX, "^2[0-9][0-9]$");
            HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration = new HttpClientConfiguration<>("dummy",
                    new OkHttpClientFactory(), MapUtils.getMapWithPrefix(new HttpConnectorConfig(config).originalsStrings(), "config.dummy."),
                    executorService, getCompositeMeterRegistry());
            HttpExchange httpExchange = getDummyHttpExchange();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(httpClientConfiguration,executorService);
            boolean success = httpConfiguration.enrichHttpExchange(httpExchange).isSuccess();
            assertThat(success).isTrue();
        }

        @Test
        void test_is_not_success_with_200_by_configuration() {
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + SUCCESS_RESPONSE_CODE_REGEX, "^1[0-9][0-9]$");
            HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration = new HttpClientConfiguration<>("dummy",
                    new OkHttpClientFactory(), MapUtils.getMapWithPrefix(new HttpConnectorConfig(config).originalsStrings(), "config.dummy."),
                    executorService, getCompositeMeterRegistry());
            HttpExchange httpExchange = getDummyHttpExchange();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(httpClientConfiguration,executorService);
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

            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + SUCCESS_RESPONSE_CODE_REGEX, "^2[0-9][0-9]$");
            HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration = new HttpClientConfiguration<>("dummy",
                    new OkHttpClientFactory(),
                    MapUtils.getMapWithPrefix(new HttpConnectorConfig(config).originalsStrings(), "config.dummy."),
                    executorService,
                    getCompositeMeterRegistry());
            HttpExchange httpExchange = getDummyHttpExchange();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(httpClientConfiguration,executorService);
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
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + SUCCESS_RESPONSE_CODE_REGEX, "^1[0-9][0-9]$");
            HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration = new HttpClientConfiguration<>(
                    "dummy", new OkHttpClientFactory(),
                    MapUtils.getMapWithPrefix(new HttpConnectorConfig(config).originalsStrings(), "config.dummy."),
                    executorService, getCompositeMeterRegistry()
            );
            HttpExchange httpExchange = getDummyHttpExchange();
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(httpClientConfiguration,executorService);
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
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + RETRY_RESPONSE_CODE_REGEX, "^5[0-9][0-9]$");
            HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration = new HttpClientConfiguration<>("dummy", new OkHttpClientFactory(),
                    MapUtils.getMapWithPrefix(new HttpConnectorConfig(config).originalsStrings(), "config.dummy."),
                    executorService,
                    getCompositeMeterRegistry());
            HttpResponse httpResponse = new HttpResponse(500, "Internal Server Error");
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(httpClientConfiguration,executorService);
            boolean retryNeeded = httpConfiguration.retryNeeded(httpResponse);
            assertThat(retryNeeded).isTrue();
        }

        @Test
        void test_retry_not_needed_with_400_status_code() {
            Map<String, String> config = Maps.newHashMap();
            config.put("httpclient.dummy." + RETRY_RESPONSE_CODE_REGEX, "^5[0-9][0-9]$");
            HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration = new HttpClientConfiguration<>("dummy", new OkHttpClientFactory(),
                    MapUtils.getMapWithPrefix(new HttpConnectorConfig(config).originalsStrings(), "config.dummy."), executorService, getCompositeMeterRegistry());
            HttpResponse httpResponse = new HttpResponse(400, "Internal Server Error");
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(httpClientConfiguration,executorService);
            boolean retryNeeded = httpConfiguration.retryNeeded(httpResponse);
            assertThat(retryNeeded).isFalse();
        }

        @Test
        void test_retry_not_needed_with_200_status_code() {
            Map<String, String> config = Maps.newHashMap();
            config.put("httpclient.dummy." + RETRY_RESPONSE_CODE_REGEX, "^5[0-9][0-9]$");
            HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration = new HttpClientConfiguration<>("dummy",
                    new OkHttpClientFactory(),
                    MapUtils.getMapWithPrefix(new HttpConnectorConfig(config).originalsStrings(), "config.dummy."),
                    executorService, getCompositeMeterRegistry());
            HttpResponse httpResponse = new HttpResponse(200, "Internal Server Error");
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(httpClientConfiguration,executorService);
            boolean retryNeeded = httpConfiguration.retryNeeded(httpResponse);
            assertThat(retryNeeded).isFalse();
        }


        @Test
        void test_retry_needed_by_configuration_with_200_status_code() {
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + RETRY_RESPONSE_CODE_REGEX, "^2[0-9][0-9]$");
            config.put(CONFIG_DEFAULT_RETRY_RESPONSE_CODE_REGEX, "^[1-5][0-9][0-9]$");
            HttpClientConfiguration<OkHttpClient, Request, Response> httpClientConfiguration = new HttpClientConfiguration<>("dummy", new OkHttpClientFactory(),
                    MapUtils.getMapWithPrefix(new HttpConnectorConfig(config).originalsStrings(), "config.dummy."), executorService, getCompositeMeterRegistry());
            HttpResponse httpResponse = new HttpResponse(200, "Internal Server Error");
            HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration = new HttpConfiguration<>(httpClientConfiguration,executorService);
            boolean retryNeeded = httpConfiguration.retryNeeded(httpResponse);
            assertThat(retryNeeded).isTrue();
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

}