package io.github.clescot.kafka.connect.http.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dev.failsafe.RateLimiter;
import io.github.clescot.kafka.connect.http.VersionUtils;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.sink.HttpSinkConnectorConfig;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.jmx.JmxMeterRegistry;
import okhttp3.Request;
import okhttp3.Response;
import org.assertj.core.util.Sets;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.clescot.kafka.connect.http.client.config.HttpRequestPredicateBuilder.*;
import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;
import static org.assertj.core.api.Assertions.assertThat;

class ConfigurationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationTest.class);
    private final ExecutorService executorService = Executors.newFixedThreadPool(2);
    private static final String DUMMY_BODY = "stuff";
    private static final String DUMMY_URL = "http://www." + DUMMY_BODY + ".com";
    private static final HttpRequest.Method DUMMY_METHOD = HttpRequest.Method.POST;
    private static final String DUMMY_BODY_TYPE = "STRING";

    @NotNull
    private CompositeMeterRegistry getCompositeMeterRegistry() {
        JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry(s -> null, Clock.SYSTEM);
        HashSet<MeterRegistry> registries = Sets.newHashSet();
        registries.add(jmxMeterRegistry);
        return new CompositeMeterRegistry(Clock.SYSTEM, registries);
    }


    @Nested
    class TestConstructor {

        @Test
        @DisplayName("test Configuration constructor with null parameters")
        void test_constructor_with_null_parameters() {
            CompositeMeterRegistry compositeMeterRegistry = getCompositeMeterRegistry();
            OkHttpClientFactory okHttpClientFactory = new OkHttpClientFactory();
            Assertions.assertThrows(NullPointerException.class, () ->
                    new Configuration<>(null, okHttpClientFactory, null, null, compositeMeterRegistry));
        }

        @Test
        @DisplayName("test Configuration constructor with url predicate")
        void test_constructor_with_url_predicate() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.test.predicate.url.regex", "^.*toto\\.com$");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration<Request, Response> configuration = new Configuration<>("test", new OkHttpClientFactory(),httpSinkConnectorConfig, executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest = new HttpRequest("http://toto.com", HttpRequest.Method.GET, HttpRequest.BodyType.STRING.name());
            assertThat(configuration.matches(httpRequest)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://titi.com", HttpRequest.Method.GET, HttpRequest.BodyType.STRING.name());
            assertThat(configuration.matches(httpRequest2)).isFalse();
        }

        @Test
        @DisplayName("test Configuration constructor with url predicate and method")
        void test_constructor_with_url_predicate_and_method() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.test.predicate.url.regex", "^.*toto\\.com$");
            settings.put("config.test.predicate.method.regex", "^GET|PUT$");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration<Request, Response> configuration = new Configuration<>("test",new OkHttpClientFactory(), httpSinkConnectorConfig, executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest = new HttpRequest("http://toto.com", HttpRequest.Method.GET, HttpRequest.BodyType.STRING.name());
            assertThat(configuration.matches(httpRequest)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://titi.com", HttpRequest.Method.GET, HttpRequest.BodyType.STRING.name());
            assertThat(configuration.matches(httpRequest2)).isFalse();
            HttpRequest httpRequest3 = new HttpRequest("http://toto.com", HttpRequest.Method.POST, HttpRequest.BodyType.STRING.name());
            assertThat(configuration.matches(httpRequest3)).isFalse();
            HttpRequest httpRequest4 = new HttpRequest("http://toto.com", HttpRequest.Method.PUT, HttpRequest.BodyType.STRING.name());
            assertThat(configuration.matches(httpRequest4)).isTrue();
        }

        @Test
        @DisplayName("test Configuration constructor with url predicate and body type")
        void test_constructor_with_url_predicate_and_bodytype() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.test.predicate.url.regex", "^.*toto\\.com$");
            settings.put("config.test.predicate.bodytype.regex", "^STRING$");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration<Request, Response> configuration = new Configuration<>("test", new OkHttpClientFactory(),httpSinkConnectorConfig, executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest1 = new HttpRequest("http://toto.com", HttpRequest.Method.GET, HttpRequest.BodyType.STRING.name());
            assertThat(configuration.matches(httpRequest1)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://toto.com", HttpRequest.Method.GET, HttpRequest.BodyType.FORM.name());
            assertThat(configuration.matches(httpRequest2)).isFalse();

        }

        @Test
        @DisplayName("test Configuration constructor with url predicate and raw header key")
        void test_constructor_with_url_predicate_and_header_key() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.test.predicate.url.regex", "^.*toto\\.com$");
            settings.put("config.test.predicate.header.key.regex", "SUPERNOVA");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration<Request, Response> configuration = new Configuration<>("test",new OkHttpClientFactory(), httpSinkConnectorConfig, executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest1 = new HttpRequest("http://toto.com", HttpRequest.Method.GET, HttpRequest.BodyType.STRING.name());
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("SUPERNOVA", List.of("stuff"));
            httpRequest1.setHeaders(headers);
            assertThat(configuration.matches(httpRequest1)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://toto.com", HttpRequest.Method.GET, HttpRequest.BodyType.FORM.name());
            assertThat(configuration.matches(httpRequest2)).isFalse();
        }

        @Test
        @DisplayName("test Configuration constructor with url predicate and header key regex")
        void test_constructor_with_url_predicate_and_header_key_regex() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.test.predicate.url.regex", "^.*toto\\.com$");
            settings.put("config.test.predicate.header.key.regex", "^SUPER.*$");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration<Request, Response> configuration = new Configuration<>("test",new OkHttpClientFactory(), httpSinkConnectorConfig, executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest1 = new HttpRequest("http://toto.com", HttpRequest.Method.GET, HttpRequest.BodyType.STRING.name());
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("SUPERNOVA", List.of("stuff"));
            httpRequest1.setHeaders(headers);
            assertThat(configuration.matches(httpRequest1)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://toto.com", HttpRequest.Method.GET, HttpRequest.BodyType.FORM.name());
            assertThat(configuration.matches(httpRequest2)).isFalse();
        }


        @Test
        @DisplayName("test Configuration constructor with url predicate and raw header key and value")
        void test_constructor_with_url_predicate_header_key_and_value() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.test.predicate.url.regex", "^.*toto\\.com$");
            settings.put("config.test.predicate.header.key.regex", "SUPERNOVA");
            settings.put("config.test.predicate.header.value.regex", "top");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration<Request, Response> configuration = new Configuration<>("test",new OkHttpClientFactory(), httpSinkConnectorConfig, executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest1 = new HttpRequest("http://toto.com", HttpRequest.Method.GET, HttpRequest.BodyType.STRING.name());
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("SUPERNOVA", List.of("top"));
            httpRequest1.setHeaders(headers);
            assertThat(configuration.matches(httpRequest1)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://toto.com", HttpRequest.Method.GET, HttpRequest.BodyType.STRING.name());
            Map<String, List<String>> headers2 = Maps.newHashMap();
            headers2.put("SUPERNOVA", List.of("tip"));
            httpRequest2.setHeaders(headers2);
            assertThat(configuration.matches(httpRequest2)).isFalse();
        }

        @Test
        @DisplayName("test Configuration constructor with url predicate and header key and value regex")
        void test_constructor_with_url_predicate_header_key_and_value_regex() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.test.predicate.url.regex", "^.*toto\\.com$");
            settings.put("config.test.predicate.header.key.regex", "^SUPER.*$");
            settings.put("config.test.predicate.header.value.regex", "^top.$");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration<Request, Response> configuration = new Configuration<>("test",new OkHttpClientFactory(), httpSinkConnectorConfig, executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest1 = new HttpRequest("http://toto.com", HttpRequest.Method.GET, HttpRequest.BodyType.STRING.name());
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("SUPERNOVA", List.of("top1"));
            httpRequest1.setHeaders(headers);
            assertThat(configuration.matches(httpRequest1)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://toto.com", HttpRequest.Method.GET, HttpRequest.BodyType.STRING.name());
            Map<String, List<String>> headers2 = Maps.newHashMap();
            headers2.put("SUPERNOVA", List.of("tip"));
            httpRequest2.setHeaders(headers2);
            assertThat(configuration.matches(httpRequest2)).isFalse();
        }

        @Test
        @DisplayName("test constructor with valid custom static headers")
        void test_constructor_with_valid_custom_static_headers() {
            Map<String, String> settings = Maps.newHashMap();
            String staticHeaderName = "toto";
            settings.put("config.default.enrich.request.static.header.names", staticHeaderName);
            String staticHeaderValue = "111-222-333";
            settings.put("config.default.enrich.request.static.header.toto", staticHeaderValue);
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            assertThat(httpSinkConnectorConfig.getStaticRequestHeaders()).containsEntry(staticHeaderName,List.of(staticHeaderValue));
        }
        @Test
        @DisplayName("test constructor with invalid custom static headers")
        void test_constructor_with_invalid_custom_static_headers() {
            Map<String, String> settings = Maps.newHashMap();
            String staticHeaderName = "toto";
            settings.put("config.default.enrich.request.static.header.names", staticHeaderName);
            String staticHeaderValue = "111-222-333";
            settings.put("config.default.enrich.request.static.header.toto2", staticHeaderValue);
            Assertions.assertThrows(NullPointerException.class,()->new HttpSinkConnectorConfig(settings));
        }
    }

    @Nested
    class TestStaticRatLimiter {

        @Test
        @DisplayName("test rate limiter with implicit instance scope")
        void test_rate_limiter_with_implicit_instance_scope() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.test.predicate.url.regex", "^.*toto\\.com$");
            settings.put("config.test.static.request.header.names", "SUPERNOVA");
            settings.put("config.test.static.request.header.names.SUPERNOVA", "top");
            settings.put("config.test.rate.limiter.max.executions", "3");
            settings.put("config.test.rate.limiter.period.in.ms", "1000");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration<Request, Response> configuration = new Configuration<>("test",new OkHttpClientFactory(), httpSinkConnectorConfig, executorService, getCompositeMeterRegistry());
            Optional<RateLimiter<HttpExchange>> rateLimiter = configuration.getHttpClient().getRateLimiter();
            assertThat(rateLimiter).isPresent();
            Configuration<Request, Response> configuration2 = new Configuration<>("test",new OkHttpClientFactory(), httpSinkConnectorConfig, executorService, getCompositeMeterRegistry());
            Optional<RateLimiter<HttpExchange>> rateLimiter2 = configuration2.getHttpClient().getRateLimiter();
            assertThat(rateLimiter2).isPresent();
            assertThat(rateLimiter.get()).isNotSameAs(rateLimiter2.get());
        }

        @Test
        @DisplayName("test rate limiter with static scope")
        void test_rate_limiter_with_static_scope() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.test.predicate.url.regex", "^.*toto\\.com$");
            settings.put("config.test.predicate.header.key", "SUPERNOVA");
            settings.put("config.test.predicate.header.value", "top");
            settings.put("config.test.rate.limiter.max.executions", "3");
            settings.put("config.test.rate.limiter.period.in.ms", "1000");
            settings.put("config.test.rate.limiter.scope", "static");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration<Request, Response> configuration = new Configuration<>("test",new OkHttpClientFactory(), httpSinkConnectorConfig, executorService, getCompositeMeterRegistry());
            Optional<RateLimiter<HttpExchange>> rateLimiter = configuration.getHttpClient().getRateLimiter();
            assertThat(rateLimiter).isPresent();
            Configuration<Request, Response> configuration2 = new Configuration<>("test",new OkHttpClientFactory(), httpSinkConnectorConfig, executorService, getCompositeMeterRegistry());
            Optional<RateLimiter<HttpExchange>> rateLimiter2 = configuration2.getHttpClient().getRateLimiter();
            assertThat(rateLimiter2).isPresent();
            assertThat(rateLimiter).containsSame(rateLimiter2.get());
        }

        @Test
        @DisplayName("test rate limiter with static scope")
        void test_rate_limiter_with_static_scope_and_different_ids() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("config.test.predicate.url.regex", "^.*toto\\.com$");
            settings.put("config.test.predicate.header.key", "SUPERNOVA");
            settings.put("config.test.predicate.header.value", "top");
            settings.put("config.test.rate.limiter.max.executions", "3");
            settings.put("config.test.rate.limiter.period.in.ms", "1000");
            settings.put("config.test.rate.limiter.scope", "static");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration<Request, Response> configuration = new Configuration<>("test",new OkHttpClientFactory(), httpSinkConnectorConfig, executorService, getCompositeMeterRegistry());
            Optional<RateLimiter<HttpExchange>> rateLimiter = configuration.getHttpClient().getRateLimiter();
            assertThat(rateLimiter).isPresent();
            Map<String, String> settings2 = Maps.newHashMap();
            settings2.put("config.test2.predicate.url.regex", "^.*toto\\.com$");
            settings2.put("config.test2.predicate.header.key", "SUPERNOVA");
            settings2.put("config.test2.predicate.header.value", "top");
            settings2.put("config.test2.rate.limiter.max.executions", "3");
            settings2.put("config.test2.rate.limiter.period.in.ms", "1000");
            settings2.put("config.test2.rate.limiter.scope", "static");
            HttpSinkConnectorConfig httpSinkConnectorConfig2 = new HttpSinkConnectorConfig(settings2);
            Configuration<Request, Response> configuration2 = new Configuration<>("test2",new OkHttpClientFactory(), httpSinkConnectorConfig2, executorService, getCompositeMeterRegistry());
            Optional<RateLimiter<HttpExchange>> rateLimiter2 = configuration2.getHttpClient().getRateLimiter();
            assertThat(rateLimiter2).isPresent();
            assertThat(rateLimiter.get()).isNotSameAs(rateLimiter2.get());
        }

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
            Configuration<Request, Response> configuration = new Configuration<>("dummy",new OkHttpClientFactory(), new HttpSinkConnectorConfig(config), executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpRequest enrichedHttpRequest = configuration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers).containsEntry("X-Stuff-Id", Lists.newArrayList("12345"))
                    .containsEntry("X-Super-Option", Lists.newArrayList("ABC"));
        }

        @Test
        void test_generate_missing_request_id() {
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + GENERATE_MISSING_REQUEST_ID, "true");
            Configuration<Request, Response> configuration = new Configuration<>("dummy",new OkHttpClientFactory(), new HttpSinkConnectorConfig(config), executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpRequest enrichedHttpRequest = configuration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers).containsKey("X-Request-ID");
        }

        @Test
        void test_generate_missing_correlation_id() {
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + GENERATE_MISSING_CORRELATION_ID, "true");
            Configuration<Request, Response> configuration = new Configuration<>("dummy",new OkHttpClientFactory(), new HttpSinkConnectorConfig(config), executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpRequest enrichedHttpRequest = configuration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers).containsKey("X-Correlation-ID");
        }

        @Test
        @DisplayName("test override User-Agent header with 'custom' value")
        void test_activating_user_agent_interceptor_with_custom_value(){

            //given
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + USER_AGENT_OVERRIDE, "custom");
            config.put("config.dummy." + USER_AGENT_CUSTOM_VALUES, "custom_ua");
            Configuration<Request, Response> configuration = new Configuration<>("dummy",new OkHttpClientFactory(), new HttpSinkConnectorConfig(config), executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpRequest enrichedHttpRequest = configuration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers).containsEntry("User-Agent", Lists.newArrayList("custom_ua"));
        }

        @Test
        @DisplayName("test override User-Agent header with multiple 'custom' value")
        void test_activating_user_agent_interceptor_with_multiple_custom_value()  {

            //given
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + USER_AGENT_OVERRIDE, "custom");
            config.put("config.dummy." + USER_AGENT_CUSTOM_VALUES, "custom_1|custom_2|custom_3");
            Configuration<Request, Response> configuration = new Configuration<>("dummy",new OkHttpClientFactory(), new HttpSinkConnectorConfig(config), executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpRequest enrichedHttpRequest = configuration.enrich(httpRequest);
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
            Configuration<Request, Response> configuration = new Configuration<>("dummy",new OkHttpClientFactory(), new HttpSinkConnectorConfig(config), executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest = getDummyHttpRequest();
            httpRequest.getHeaders().put("User-Agent", Lists.newArrayList("already"));
            HttpRequest enrichedHttpRequest = configuration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers.get("User-Agent").get(0)).isEqualTo("already");
        }

        @Test
        @DisplayName("test override User-Agent header with 'http_client' settings")
        void test_activating_user_agent_interceptor_with_http_client_scope()  {

            //given
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + USER_AGENT_OVERRIDE, "http_client");
            Configuration<Request, Response> configuration = new Configuration<>("dummy",new OkHttpClientFactory(), new HttpSinkConnectorConfig(config), executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpRequest enrichedHttpRequest = configuration.enrich(httpRequest);
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
            Configuration<Request, Response> configuration = new Configuration<>("dummy",new OkHttpClientFactory(), new HttpSinkConnectorConfig(config), executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpRequest enrichedHttpRequest = configuration.enrich(httpRequest);
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
            Configuration<Request, Response> configuration = new Configuration<>("dummy",new OkHttpClientFactory(), new HttpSinkConnectorConfig(config), executorService, getCompositeMeterRegistry());
            HttpExchange httpExchange = getDummyHttpExchange();
            boolean success = configuration.enrichHttpExchange(httpExchange).isSuccess();
            assertThat(success).isTrue();
        }

        @Test
        void test_is_not_success_with_200_by_configuration() {
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + SUCCESS_RESPONSE_CODE_REGEX, "^1[0-9][0-9]$");
            Configuration<Request, Response> configuration = new Configuration<>("dummy",new OkHttpClientFactory(), new HttpSinkConnectorConfig(config), executorService, getCompositeMeterRegistry());
            HttpExchange httpExchange = getDummyHttpExchange();
            boolean success = configuration.enrichHttpExchange(httpExchange).isSuccess();
            assertThat(success).isFalse();
        }

    }

    @Nested
    class RetryNeeded {
        @Test
        void test_retry_needed() {
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + RETRY_RESPONSE_CODE_REGEX, "^5[0-9][0-9]$");
            Configuration<Request, Response> configuration = new Configuration<>("dummy",new OkHttpClientFactory(), new HttpSinkConnectorConfig(config), executorService, getCompositeMeterRegistry());
            HttpResponse httpResponse = new HttpResponse(500, "Internal Server Error");
            boolean retryNeeded = configuration.retryNeeded(httpResponse);
            assertThat(retryNeeded).isTrue();
        }

        @Test
        void test_retry_not_needed_with_400_status_code() {
            Map<String, String> config = Maps.newHashMap();
            config.put("httpclient.dummy." + RETRY_RESPONSE_CODE_REGEX, "^5[0-9][0-9]$");
            Configuration<Request, Response> configuration = new Configuration<>("dummy",new OkHttpClientFactory(), new HttpSinkConnectorConfig(config), executorService, getCompositeMeterRegistry());
            HttpResponse httpResponse = new HttpResponse(400, "Internal Server Error");
            boolean retryNeeded = configuration.retryNeeded(httpResponse);
            assertThat(retryNeeded).isFalse();
        }

        @Test
        void test_retry_not_needed_with_200_status_code() {
            Map<String, String> config = Maps.newHashMap();
            config.put("httpclient.dummy." + RETRY_RESPONSE_CODE_REGEX, "^5[0-9][0-9]$");
            Configuration<Request, Response> configuration = new Configuration<>("dummy",new OkHttpClientFactory(), new HttpSinkConnectorConfig(config), executorService, getCompositeMeterRegistry());
            HttpResponse httpResponse = new HttpResponse(200, "Internal Server Error");
            boolean retryNeeded = configuration.retryNeeded(httpResponse);
            assertThat(retryNeeded).isFalse();
        }


        @Test
        void test_retry_needed_by_configuration_with_200_status_code() {
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + RETRY_RESPONSE_CODE_REGEX, "^2[0-9][0-9]$");
            config.put(CONFIG_DEFAULT_RETRY_RESPONSE_CODE_REGEX, "^[1-5][0-9][0-9]$");
            Configuration<Request, Response> configuration = new Configuration<>("dummy",new OkHttpClientFactory(), new HttpSinkConnectorConfig(config), executorService, getCompositeMeterRegistry());
            HttpResponse httpResponse = new HttpResponse(200, "Internal Server Error");

            boolean retryNeeded = configuration.retryNeeded(httpResponse);
            assertThat(retryNeeded).isTrue();
        }
    }


    @Nested
    class TestToString {
        @Test
        void test_with_all_fields_set() {
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + RETRY_RESPONSE_CODE_REGEX, "^2[0-9][0-9]$");
            config.put(CONFIG_DEFAULT_RETRY_RESPONSE_CODE_REGEX, "^[1-5][0-9][0-9]$");
            config.put("config.dummy.retry.policy.retries", "3");
            config.put("config.dummy.retry.delay.in.ms", "600");
            config.put("config.dummy.retry.max.delay.in.ms", "1200");
            config.put("config.dummy.retry.delay.factor", "1.2");
            config.put("config.dummy.retry.jitter.in.ms", "200");
            config.put("config.dummy." + URL_REGEX, ".*");
            config.put("config.dummy." + METHOD_REGEX, "GET");
            config.put("config.dummy." + BODYTYPE_REGEX, "STRING");
            config.put("config.dummy." + HEADER_KEY_REGEX, ".*");
            config.put("config.dummy." + HEADER_VALUE_REGEX, ".*");
            config.put("config.dummy." + STATIC_REQUEST_HEADER_NAMES, "headerName1,headerName2");
            config.put("config.dummy." + STATIC_REQUEST_HEADER_PREFIX + "headerName1", "value1");
            config.put("config.dummy." + STATIC_REQUEST_HEADER_PREFIX + "headerName2", "value2");
            config.put("config.dummy." + RATE_LIMITER_MAX_EXECUTIONS, "4");
            config.put("config.dummy." + RATE_LIMITER_PERIOD_IN_MS, "1000");
            config.put("config.dummy." + RATE_LIMITER_SCOPE, "static");
            Configuration<Request, Response> configuration = new Configuration<>("dummy",new OkHttpClientFactory(), new HttpSinkConnectorConfig(config), executorService, getCompositeMeterRegistry());
            String configurationAsString = configuration.toString();
            LOGGER.debug("configurationAsString:{}", configurationAsString);
            assertThat(configurationAsString).isNotEmpty();
        }
    }

    @Nested
    class AddSuccessStatusToHttpExchangeFunction{
        private final HttpRequest.Method dummyMethod = HttpRequest.Method.POST;
        private static final String DUMMY_BODY_TYPE = "STRING";

        private final ExecutorService executorService = Executors.newFixedThreadPool(2);

        @Test
        void test_is_success_with_200() {

            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + SUCCESS_RESPONSE_CODE_REGEX, "^2[0-9][0-9]$");
            Configuration<Request,Response> configuration = new Configuration<>("dummy", new OkHttpClientFactory(), new HttpSinkConnectorConfig(config), executorService, getCompositeMeterRegistry());
            HttpExchange httpExchange = getDummyHttpExchange();
            boolean success = configuration.enrichHttpExchange(httpExchange).isSuccess();
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
            Configuration<Request,Response> configuration = new Configuration<>("dummy",new OkHttpClientFactory(), new HttpSinkConnectorConfig(config), executorService, getCompositeMeterRegistry());
            HttpExchange httpExchange = getDummyHttpExchange();
            boolean success = configuration.enrichHttpExchange(httpExchange).isSuccess();
            assertThat(success).isFalse();
        }



        private HttpExchange getDummyHttpExchange() {
            Map<String, List<String>> requestHeaders = Maps.newHashMap();
            requestHeaders.put("X-dummy", Lists.newArrayList("blabla"));
            HttpRequest httpRequest = new HttpRequest("http://www.titi.com", dummyMethod, DUMMY_BODY_TYPE);
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
        HttpRequest httpRequest = new HttpRequest(DUMMY_URL, DUMMY_METHOD, DUMMY_BODY_TYPE);
        Map<String, List<String>> headers = Maps.newHashMap();
        headers.put("Content-Type", Lists.newArrayList("application/json"));
        httpRequest.setHeaders(headers);
        httpRequest.setBodyAsString("stuff");
        httpRequest.setBodyAsForm(Maps.newHashMap());
        return httpRequest;
    }
}