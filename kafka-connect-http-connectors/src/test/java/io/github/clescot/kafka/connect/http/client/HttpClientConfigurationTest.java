package io.github.clescot.kafka.connect.http.client;

import com.google.common.collect.Maps;
import dev.failsafe.RateLimiter;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.sink.HttpConnectorConfig;
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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.github.clescot.kafka.connect.http.client.config.HttpRequestPredicateBuilder.*;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.*;
import static org.assertj.core.api.Assertions.assertThat;

class HttpClientConfigurationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientConfigurationTest.class);
    private final ExecutorService executorService = Executors.newFixedThreadPool(2);
    private static final String DUMMY_BODY = "stuff";
    private static final String DUMMY_URL = "http://www." + DUMMY_BODY + ".com";
    private static final HttpRequest.Method DUMMY_METHOD = HttpRequest.Method.POST;

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
                    new HttpClientConfiguration<>(null, okHttpClientFactory, null, null, compositeMeterRegistry));
        }

        @Test
        @DisplayName("test Configuration constructor with url predicate")
        void test_constructor_with_url_predicate() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("predicate.url.regex", "^.*toto\\.com$");
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(settings);
            HttpClientConfiguration<OkHttpClient,Request, Response> httpClientConfiguration = new HttpClientConfiguration<>("test", new OkHttpClientFactory(), httpConnectorConfig.originals(), executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest = new HttpRequest("http://toto.com", HttpRequest.Method.GET);
            assertThat(httpClientConfiguration.matches(httpRequest)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://titi.com", HttpRequest.Method.GET);
            assertThat(httpClientConfiguration.matches(httpRequest2)).isFalse();
        }

        @Test
        @DisplayName("test Configuration constructor with url predicate and method")
        void test_constructor_with_url_predicate_and_method() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("predicate.url.regex", "^.*toto\\.com$");
            settings.put("predicate.method.regex", "^GET|PUT$");
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(settings);
            HttpClientConfiguration<OkHttpClient,Request,Response> httpClientConfiguration = new HttpClientConfiguration<>("test",new OkHttpClientFactory(), httpConnectorConfig.originals(), executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest = new HttpRequest("http://toto.com", HttpRequest.Method.GET);
            assertThat(httpClientConfiguration.matches(httpRequest)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://titi.com", HttpRequest.Method.GET);
            assertThat(httpClientConfiguration.matches(httpRequest2)).isFalse();
            HttpRequest httpRequest3 = new HttpRequest("http://toto.com", HttpRequest.Method.POST);
            assertThat(httpClientConfiguration.matches(httpRequest3)).isFalse();
            HttpRequest httpRequest4 = new HttpRequest("http://toto.com", HttpRequest.Method.PUT);
            assertThat(httpClientConfiguration.matches(httpRequest4)).isTrue();
        }

        @Test
        @DisplayName("test Configuration constructor with url predicate and body type")
        void test_constructor_with_url_predicate_and_bodytype() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("predicate.url.regex", "^.*toto\\.com$");
            settings.put("predicate.bodytype.regex", "^STRING$");
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(settings);
            HttpClientConfiguration<OkHttpClient,Request,Response> httpClientConfiguration = new HttpClientConfiguration<>("test", new OkHttpClientFactory(), httpConnectorConfig.originals(), executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest1 = new HttpRequest("http://toto.com", HttpRequest.Method.GET);
            assertThat(httpClientConfiguration.matches(httpRequest1)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://titi.com", HttpRequest.Method.GET);
            assertThat(httpClientConfiguration.matches(httpRequest2)).isFalse();

        }

        @Test
        @DisplayName("test Configuration constructor with url predicate and raw header key")
        void test_constructor_with_url_predicate_and_header_key() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("predicate.url.regex", "^.*toto\\.com$");
            settings.put("predicate.header.key.regex", "SUPERNOVA");
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(settings);
            HttpClientConfiguration<OkHttpClient,Request,Response> httpClientConfiguration = new HttpClientConfiguration<>("test",new OkHttpClientFactory(), httpConnectorConfig.originals(), executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest1 = new HttpRequest("http://toto.com", HttpRequest.Method.GET);
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("SUPERNOVA", List.of("stuff"));
            httpRequest1.setHeaders(headers);
            assertThat(httpClientConfiguration.matches(httpRequest1)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://toto.com", HttpRequest.Method.GET);
            assertThat(httpClientConfiguration.matches(httpRequest2)).isFalse();
        }

        @Test
        @DisplayName("test Configuration constructor with url predicate and header key regex")
        void test_constructor_with_url_predicate_and_header_key_regex() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("predicate.url.regex", "^.*toto\\.com$");
            settings.put("predicate.header.key.regex", "^SUPER.*$");
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(settings);
            HttpClientConfiguration<OkHttpClient,Request,Response> httpClientConfiguration = new HttpClientConfiguration<>("test",new OkHttpClientFactory(), httpConnectorConfig.originals(), executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest1 = new HttpRequest("http://toto.com", HttpRequest.Method.GET);
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("SUPERNOVA", List.of("stuff"));
            httpRequest1.setHeaders(headers);
            assertThat(httpClientConfiguration.matches(httpRequest1)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://toto.com", HttpRequest.Method.GET);
            assertThat(httpClientConfiguration.matches(httpRequest2)).isFalse();
        }


        @Test
        @DisplayName("test Configuration constructor with url predicate and raw header key and value")
        void test_constructor_with_url_predicate_header_key_and_value() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("predicate.url.regex", "^.*toto\\.com$");
            settings.put("predicate.header.key.regex", "SUPERNOVA");
            settings.put("predicate.header.value.regex", "top");
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(settings);
            HttpClientConfiguration<OkHttpClient,Request,Response> httpClientConfiguration = new HttpClientConfiguration<>("test",new OkHttpClientFactory(), httpConnectorConfig.originals(), executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest1 = new HttpRequest("http://toto.com", HttpRequest.Method.GET);
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("SUPERNOVA", List.of("top"));
            httpRequest1.setHeaders(headers);
            assertThat(httpClientConfiguration.matches(httpRequest1)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://toto.com", HttpRequest.Method.GET);
            Map<String, List<String>> headers2 = Maps.newHashMap();
            headers2.put("SUPERNOVA", List.of("tip"));
            httpRequest2.setHeaders(headers2);
            assertThat(httpClientConfiguration.matches(httpRequest2)).isFalse();
        }

        @Test
        @DisplayName("test Configuration constructor with url predicate and header key and value regex")
        void test_constructor_with_url_predicate_header_key_and_value_regex() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("predicate.url.regex", "^.*toto\\.com$");
            settings.put("predicate.header.key.regex", "^SUPER.*$");
            settings.put("predicate.header.value.regex", "^top.$");
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(settings);
            HttpClientConfiguration<OkHttpClient,Request,Response> httpClientConfiguration = new HttpClientConfiguration<>("test",new OkHttpClientFactory(), httpConnectorConfig.originals(), executorService, getCompositeMeterRegistry());
            HttpRequest httpRequest1 = new HttpRequest("http://toto.com", HttpRequest.Method.GET);
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("SUPERNOVA", List.of("top1"));
            httpRequest1.setHeaders(headers);
            assertThat(httpClientConfiguration.matches(httpRequest1)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://toto.com", HttpRequest.Method.GET);
            Map<String, List<String>> headers2 = Maps.newHashMap();
            headers2.put("SUPERNOVA", List.of("tip"));
            httpRequest2.setHeaders(headers2);
            assertThat(httpClientConfiguration.matches(httpRequest2)).isFalse();
        }

        @Test
        @DisplayName("test constructor with valid custom static headers")
        void test_constructor_with_valid_custom_static_headers() {
            Map<String, String> settings = Maps.newHashMap();
            String staticHeaderName = "toto";
            settings.put("config.default.enrich.request.static.header.names", staticHeaderName);
            String staticHeaderValue = "111-222-333";
            settings.put("config.default.enrich.request.static.header.toto", staticHeaderValue);
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(settings);
            assertThat(httpConnectorConfig.getStaticRequestHeaders()).containsEntry(staticHeaderName,List.of(staticHeaderValue));
        }
        @Test
        @DisplayName("test constructor with invalid custom static headers")
        void test_constructor_with_invalid_custom_static_headers() {
            Map<String, String> settings = Maps.newHashMap();
            String staticHeaderName = "toto";
            settings.put("config.default.enrich.request.static.header.names", staticHeaderName);
            String staticHeaderValue = "111-222-333";
            settings.put("config.default.enrich.request.static.header.toto2", staticHeaderValue);
            Assertions.assertThrows(NullPointerException.class,()->new HttpConnectorConfig(settings));
        }
    }

    @Nested
    class TestStaticRatLimiter {

        @Test
        @DisplayName("test rate limiter with implicit instance scope")
        void test_rate_limiter_with_implicit_instance_scope() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("predicate.url.regex", "^.*toto\\.com$");
            settings.put("static.request.header.names", "SUPERNOVA");
            settings.put("static.request.header.names.SUPERNOVA", "top");
            settings.put("rate.limiter.max.executions", "3");
            settings.put("rate.limiter.period.in.ms", "1000");
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(settings);
            HttpClientConfiguration<OkHttpClient,Request,Response> httpClientConfiguration = new HttpClientConfiguration<>("test",new OkHttpClientFactory(), httpConnectorConfig.originals(), executorService, getCompositeMeterRegistry());
            Optional<RateLimiter<HttpExchange>> rateLimiter = httpClientConfiguration.getClient().getRateLimiter();
            assertThat(rateLimiter).isPresent();
            HttpClientConfiguration<OkHttpClient,Request,Response> httpClientConfiguration2 = new HttpClientConfiguration<>("test",new OkHttpClientFactory(), httpConnectorConfig.originals(), executorService, getCompositeMeterRegistry());
            Optional<RateLimiter<HttpExchange>> rateLimiter2 = httpClientConfiguration2.getClient().getRateLimiter();
            assertThat(rateLimiter2).isPresent();
            assertThat(rateLimiter.get()).isNotSameAs(rateLimiter2.get());
        }

        @Test
        @DisplayName("test rate limiter with static scope")
        void test_rate_limiter_with_static_scope() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("predicate.url.regex", "^.*toto\\.com$");
            settings.put("predicate.header.key", "SUPERNOVA");
            settings.put("predicate.header.value", "top");
            settings.put("rate.limiter.max.executions", "3");
            settings.put("rate.limiter.period.in.ms", "1000");
            settings.put("rate.limiter.scope", "static");
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(settings);
            HttpClientConfiguration<OkHttpClient,Request,Response> httpClientConfiguration = new HttpClientConfiguration<>("test",new OkHttpClientFactory(), httpConnectorConfig.originals(), executorService, getCompositeMeterRegistry());
            Optional<RateLimiter<HttpExchange>> rateLimiter = httpClientConfiguration.getClient().getRateLimiter();
            assertThat(rateLimiter).isPresent();
            HttpClientConfiguration<OkHttpClient,Request,Response> httpClientConfiguration2 = new HttpClientConfiguration<>("test",new OkHttpClientFactory(), httpConnectorConfig.originals(), executorService, getCompositeMeterRegistry());
            Optional<RateLimiter<HttpExchange>> rateLimiter2 = httpClientConfiguration2.getClient().getRateLimiter();
            assertThat(rateLimiter2).isPresent();
            assertThat(rateLimiter).containsSame(rateLimiter2.get());
        }

        @Test
        @DisplayName("test rate limiter with static scope")
        void test_rate_limiter_with_static_scope_and_different_ids() {
            Map<String, String> settings = Maps.newHashMap();
            settings.put("predicate.url.regex", "^.*toto\\.com$");
            settings.put("predicate.header.key", "SUPERNOVA");
            settings.put("predicate.header.value", "top");
            settings.put("rate.limiter.max.executions", "3");
            settings.put("rate.limiter.period.in.ms", "1000");
            settings.put("rate.limiter.scope", "static");
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(settings);
            HttpClientConfiguration<OkHttpClient,Request,Response> httpClientConfiguration = new HttpClientConfiguration<>("test",new OkHttpClientFactory(), httpConnectorConfig.originals(), executorService, getCompositeMeterRegistry());
            Optional<RateLimiter<HttpExchange>> rateLimiter = httpClientConfiguration.getClient().getRateLimiter();
            assertThat(rateLimiter).isPresent();
            Map<String, String> settings2 = Maps.newHashMap();
            settings2.put("predicate.url.regex", "^.*toto\\.com$");
            settings2.put("predicate.header.key", "SUPERNOVA");
            settings2.put("predicate.header.value", "top");
            settings2.put("rate.limiter.max.executions", "3");
            settings2.put("rate.limiter.period.in.ms", "1000");
            settings2.put("rate.limiter.scope", "static");
            HttpConnectorConfig httpConnectorConfig2 = new HttpConnectorConfig(settings2);
            HttpClientConfiguration<OkHttpClient,Request,Response> httpClientConfiguration2 = new HttpClientConfiguration<>("test2",new OkHttpClientFactory(), httpConnectorConfig2.originals(), executorService, getCompositeMeterRegistry());
            Optional<RateLimiter<HttpExchange>> rateLimiter2 = httpClientConfiguration2.getClient().getRateLimiter();
            assertThat(rateLimiter2).isPresent();
            assertThat(rateLimiter.get()).isNotSameAs(rateLimiter2.get());
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
            HttpClientConfiguration<OkHttpClient,Request,Response> httpClientConfiguration = new HttpClientConfiguration<>("dummy",new OkHttpClientFactory(), new HttpConnectorConfig(config).originals(), executorService, getCompositeMeterRegistry());
            String configurationAsString = httpClientConfiguration.toString();
            LOGGER.debug("configurationAsString:{}", configurationAsString);
            assertThat(configurationAsString).isNotEmpty();
        }
    }



}