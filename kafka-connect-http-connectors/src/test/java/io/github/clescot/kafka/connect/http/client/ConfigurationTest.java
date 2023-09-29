package io.github.clescot.kafka.connect.http.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dev.failsafe.RateLimiter;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.sink.HttpSinkConnectorConfig;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.jmx.JmxMeterRegistry;
import org.assertj.core.util.Sets;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;
import static org.assertj.core.api.Assertions.assertThat;

class ConfigurationTest {
    private ExecutorService executorService = Executors.newFixedThreadPool(2);
    private static final String DUMMY_BODY = "stuff";
    private static final String DUMMY_URL = "http://www." + DUMMY_BODY + ".com";
    private static final String DUMMY_METHOD = "POST";
    private static final String DUMMY_BODY_TYPE = "STRING";

    @NotNull
    private CompositeMeterRegistry getCompositeMeterRegistry() {
        JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry(s -> null, Clock.SYSTEM);
        HashSet<MeterRegistry> registries = Sets.newHashSet();
        registries.add(jmxMeterRegistry);
        return new CompositeMeterRegistry(Clock.SYSTEM, registries);
    }


    @Nested
    class TestConstructor{

        @Test
        @DisplayName("test Configuration constructor with null parameters")
        public void test_constructor_with_null_parameters(){
            Assertions.assertThrows(NullPointerException.class,()->
            new Configuration(null,null,null,getCompositeMeterRegistry()));
        }
        @Test
        @DisplayName("test Configuration constructor with url predicate")
        public void test_constructor_with_url_predicate(){
            Map<String,String> settings = Maps.newHashMap();
            settings.put("config.test.predicate.url.regex","^.*toto\\.com$");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration configuration = new Configuration("test", httpSinkConnectorConfig,executorService,getCompositeMeterRegistry());
            HttpRequest httpRequest = new HttpRequest("http://toto.com","GET", HttpRequest.BodyType.STRING.name());
            assertThat(configuration.matches(httpRequest)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://titi.com","GET", HttpRequest.BodyType.STRING.name());
            assertThat(configuration.matches(httpRequest2)).isFalse();
        }

        @Test
        @DisplayName("test Configuration constructor with url predicate and method")
        public void test_constructor_with_url_predicate_and_method(){
            Map<String,String> settings = Maps.newHashMap();
            settings.put("config.test.predicate.url.regex","^.*toto\\.com$");
            settings.put("config.test.predicate.method.regex","^GET|PUT$");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration configuration = new Configuration("test", httpSinkConnectorConfig,executorService,getCompositeMeterRegistry());
            HttpRequest httpRequest = new HttpRequest("http://toto.com","GET", HttpRequest.BodyType.STRING.name());
            assertThat(configuration.matches(httpRequest)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://titi.com","GET", HttpRequest.BodyType.STRING.name());
            assertThat(configuration.matches(httpRequest2)).isFalse();
            HttpRequest httpRequest3 = new HttpRequest("http://toto.com","POST", HttpRequest.BodyType.STRING.name());
            assertThat(configuration.matches(httpRequest3)).isFalse();
            HttpRequest httpRequest4 = new HttpRequest("http://toto.com","PUT", HttpRequest.BodyType.STRING.name());
            assertThat(configuration.matches(httpRequest4)).isTrue();
        }

        @Test
        @DisplayName("test Configuration constructor with url predicate and body type")
        public void test_constructor_with_url_predicate_and_bodytype(){
            Map<String,String> settings = Maps.newHashMap();
            settings.put("config.test.predicate.url.regex","^.*toto\\.com$");
            settings.put("config.test.predicate.bodytype.regex","^STRING$");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration configuration = new Configuration("test", httpSinkConnectorConfig,executorService,getCompositeMeterRegistry());
            HttpRequest httpRequest1 = new HttpRequest("http://toto.com","GET", HttpRequest.BodyType.STRING.name());
            assertThat(configuration.matches(httpRequest1)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://toto.com","GET", HttpRequest.BodyType.FORM.name());
            assertThat(configuration.matches(httpRequest2)).isFalse();

        }

        @Test
        @DisplayName("test Configuration constructor with url predicate and raw header key")
        public void test_constructor_with_url_predicate_and_header_key(){
            Map<String,String> settings = Maps.newHashMap();
            settings.put("config.test.predicate.url.regex","^.*toto\\.com$");
            settings.put("config.test.predicate.header.key.regex","SUPERNOVA");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration configuration = new Configuration("test", httpSinkConnectorConfig,executorService,getCompositeMeterRegistry());
            HttpRequest httpRequest1 = new HttpRequest("http://toto.com","GET", HttpRequest.BodyType.STRING.name());
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("SUPERNOVA", List.of("stuff"));
            httpRequest1.setHeaders(headers);
            assertThat(configuration.matches(httpRequest1)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://toto.com","GET", HttpRequest.BodyType.FORM.name());
            assertThat(configuration.matches(httpRequest2)).isFalse();
        }

        @Test
        @DisplayName("test Configuration constructor with url predicate and header key regex")
        public void test_constructor_with_url_predicate_and_header_key_regex(){
            Map<String,String> settings = Maps.newHashMap();
            settings.put("config.test.predicate.url.regex","^.*toto\\.com$");
            settings.put("config.test.predicate.header.key.regex","^SUPER.*$");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration configuration = new Configuration("test", httpSinkConnectorConfig,executorService,getCompositeMeterRegistry());
            HttpRequest httpRequest1 = new HttpRequest("http://toto.com","GET", HttpRequest.BodyType.STRING.name());
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("SUPERNOVA", List.of("stuff"));
            httpRequest1.setHeaders(headers);
            assertThat(configuration.matches(httpRequest1)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://toto.com","GET", HttpRequest.BodyType.FORM.name());
            assertThat(configuration.matches(httpRequest2)).isFalse();
        }


        @Test
        @DisplayName("test Configuration constructor with url predicate and raw header key and value")
        public void test_constructor_with_url_predicate_header_key_and_value(){
            Map<String,String> settings = Maps.newHashMap();
            settings.put("config.test.predicate.url.regex","^.*toto\\.com$");
            settings.put("config.test.predicate.header.key.regex","SUPERNOVA");
            settings.put("config.test.predicate.header.value.regex","top");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration configuration = new Configuration("test", httpSinkConnectorConfig,executorService,getCompositeMeterRegistry());
            HttpRequest httpRequest1 = new HttpRequest("http://toto.com","GET", HttpRequest.BodyType.STRING.name());
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("SUPERNOVA", List.of("top"));
            httpRequest1.setHeaders(headers);
            assertThat(configuration.matches(httpRequest1)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://toto.com","GET", HttpRequest.BodyType.STRING.name());
            Map<String, List<String>> headers2 = Maps.newHashMap();
            headers2.put("SUPERNOVA", List.of("tip"));
            httpRequest2.setHeaders(headers2);
            assertThat(configuration.matches(httpRequest2)).isFalse();
        }

        @Test
        @DisplayName("test Configuration constructor with url predicate and header key and value regex")
        public void test_constructor_with_url_predicate_header_key_and_value_regex(){
            Map<String,String> settings = Maps.newHashMap();
            settings.put("config.test.predicate.url.regex","^.*toto\\.com$");
            settings.put("config.test.predicate.header.key.regex","^SUPER.*$");
            settings.put("config.test.predicate.header.value.regex","^top.$");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration configuration = new Configuration("test", httpSinkConnectorConfig,executorService,getCompositeMeterRegistry());
            HttpRequest httpRequest1 = new HttpRequest("http://toto.com","GET", HttpRequest.BodyType.STRING.name());
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("SUPERNOVA", List.of("top1"));
            httpRequest1.setHeaders(headers);
            assertThat(configuration.matches(httpRequest1)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://toto.com","GET", HttpRequest.BodyType.STRING.name());
            Map<String, List<String>> headers2 = Maps.newHashMap();
            headers2.put("SUPERNOVA", List.of("tip"));
            httpRequest2.setHeaders(headers2);
            assertThat(configuration.matches(httpRequest2)).isFalse();
        }
    }

    @Nested
    class TestStaticRatLimiter{

        @Test
        @DisplayName("test rate limiter with implicit instance scope")
        public void test_rate_limiter_with_implicit_instance_scope(){
            Map<String,String> settings = Maps.newHashMap();
            settings.put("config.test.predicate.url.regex","^.*toto\\.com$");
            settings.put("config.test.static.request.header.names","SUPERNOVA");
            settings.put("config.test.static.request.header.names.SUPERNOVA","top");
            settings.put("config.test.rate.limiter.max.executions","3");
            settings.put("config.test.rate.limiter.period.in.ms","1000");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration configuration = new Configuration("test", httpSinkConnectorConfig,executorService,getCompositeMeterRegistry());
            Optional<RateLimiter<HttpExchange>> rateLimiter = configuration.getHttpClient().getRateLimiter();
            assertThat(rateLimiter.isPresent()).isTrue();
            Configuration configuration2 = new Configuration("test", httpSinkConnectorConfig,executorService,getCompositeMeterRegistry());
            Optional<RateLimiter<HttpExchange>> rateLimiter2 = configuration2.getHttpClient().getRateLimiter();
            assertThat(rateLimiter2.isPresent()).isTrue();
            assertThat(rateLimiter.get()!=rateLimiter2.get()).isTrue();
        }

        @Test
        @DisplayName("test rate limiter with static scope")
        public void test_rate_limiter_with_static_scope(){
            Map<String,String> settings = Maps.newHashMap();
            settings.put("config.test.predicate.url.regex","^.*toto\\.com$");
            settings.put("config.test.predicate.header.key","SUPERNOVA");
            settings.put("config.test.predicate.header.value","top");
            settings.put("config.test.rate.limiter.max.executions","3");
            settings.put("config.test.rate.limiter.period.in.ms","1000");
            settings.put("config.test.rate.limiter.scope","static");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration configuration = new Configuration("test", httpSinkConnectorConfig,executorService,getCompositeMeterRegistry());
            Optional<RateLimiter<HttpExchange>> rateLimiter = configuration.getHttpClient().getRateLimiter();
            assertThat(rateLimiter.isPresent()).isTrue();
            Configuration configuration2 = new Configuration("test", httpSinkConnectorConfig,executorService,getCompositeMeterRegistry());
            Optional<RateLimiter<HttpExchange>> rateLimiter2 = configuration2.getHttpClient().getRateLimiter();
            assertThat(rateLimiter2.isPresent()).isTrue();
            assertThat(rateLimiter.get()==rateLimiter2.get()).isTrue();
        }
        @Test
        @DisplayName("test rate limiter with static scope")
        public void test_rate_limiter_with_static_scope_and_different_ids(){
            Map<String,String> settings = Maps.newHashMap();
            settings.put("config.test.predicate.url.regex","^.*toto\\.com$");
            settings.put("config.test.predicate.header.key","SUPERNOVA");
            settings.put("config.test.predicate.header.value","top");
            settings.put("config.test.rate.limiter.max.executions","3");
            settings.put("config.test.rate.limiter.period.in.ms","1000");
            settings.put("config.test.rate.limiter.scope","static");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration configuration = new Configuration("test", httpSinkConnectorConfig,executorService,getCompositeMeterRegistry());
            Optional<RateLimiter<HttpExchange>> rateLimiter = configuration.getHttpClient().getRateLimiter();
            assertThat(rateLimiter.isPresent()).isTrue();
            Map<String,String> settings2 = Maps.newHashMap();
            settings2.put("config.test2.predicate.url.regex","^.*toto\\.com$");
            settings2.put("config.test2.predicate.header.key","SUPERNOVA");
            settings2.put("config.test2.predicate.header.value","top");
            settings2.put("config.test2.rate.limiter.max.executions","3");
            settings2.put("config.test2.rate.limiter.period.in.ms","1000");
            settings2.put("config.test2.rate.limiter.scope","static");
            HttpSinkConnectorConfig httpSinkConnectorConfig2 = new HttpSinkConnectorConfig(settings2);
            Configuration configuration2 = new Configuration("test2", httpSinkConnectorConfig2,executorService,getCompositeMeterRegistry());
            Optional<RateLimiter<HttpExchange>> rateLimiter2 = configuration2.getHttpClient().getRateLimiter();
            assertThat(rateLimiter2.isPresent()).isTrue();
            assertThat(rateLimiter.get()!=rateLimiter2.get()).isTrue();
        }

    }

    @Nested
    class TestEnrichHttpRequest{
        private ExecutorService executorService = Executors.newFixedThreadPool(2);
        @Test
        public void test_add_static_headers(){
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + STATIC_REQUEST_HEADER_NAMES, "X-Stuff-Id,X-Super-Option");
            config.put("config.dummy." + STATIC_REQUEST_HEADER_PREFIX+"X-Stuff-Id","12345");
            config.put("config.dummy." + STATIC_REQUEST_HEADER_PREFIX+"X-Super-Option","ABC");
            Configuration configuration = new Configuration("dummy", new HttpSinkConnectorConfig(config), executorService,getCompositeMeterRegistry());
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpRequest enrichedHttpRequest = configuration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers).containsEntry("X-Stuff-Id",Lists.newArrayList("12345"));
            assertThat(headers).containsEntry("X-Super-Option",Lists.newArrayList("ABC"));
        }
        @Test
        public void test_generate_missing_request_id(){
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + GENERATE_MISSING_REQUEST_ID, "true");
            Configuration configuration = new Configuration("dummy", new HttpSinkConnectorConfig(config), executorService,getCompositeMeterRegistry());
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpRequest enrichedHttpRequest = configuration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers).containsKey("X-Request-ID");;
        }
        @Test
        public void test_generate_missing_correlation_id(){
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + GENERATE_MISSING_CORRELATION_ID, "true");
            Configuration configuration = new Configuration("dummy", new HttpSinkConnectorConfig(config), executorService,getCompositeMeterRegistry());
            HttpRequest httpRequest = getDummyHttpRequest();
            HttpRequest enrichedHttpRequest = configuration.enrich(httpRequest);
            Map<String, List<String>> headers = enrichedHttpRequest.getHeaders();
            assertThat(headers).containsKey("X-Correlation-ID");;
        }
    }

    @Nested
    class TestEnrichHttpExchange{

        private ExecutorService executorService = Executors.newFixedThreadPool(2);

        @Test
        public void test_is_success_with_200() {

            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + SUCCESS_RESPONSE_CODE_REGEX, "^2[0-9][0-9]$");
            Configuration configuration = new Configuration("dummy", new HttpSinkConnectorConfig(config), executorService,getCompositeMeterRegistry());
            HttpExchange httpExchange = getDummyHttpExchange();
            boolean success = configuration.enrich(httpExchange).isSuccess();
            assertThat(success).isTrue();
        }

        @Test
        public void test_is_not_success_with_200_by_configuration() {
            Map<String, String> config = Maps.newHashMap();
            config.put("config.dummy." + SUCCESS_RESPONSE_CODE_REGEX, "^1[0-9][0-9]$");
            Configuration configuration = new Configuration("dummy", new HttpSinkConnectorConfig(config), executorService,getCompositeMeterRegistry());
            HttpExchange httpExchange = getDummyHttpExchange();
            boolean success = configuration.enrich(httpExchange).isSuccess();
            assertThat(success).isFalse();
        }

    }

    @Nested
    class RetryNeeded{
        @Test
        public void test_retry_needed() {
            Map<String,String> config = Maps.newHashMap();
            config.put("config.dummy."+RETRY_RESPONSE_CODE_REGEX,"^5[0-9][0-9]$");
            Configuration configuration = new Configuration("dummy",new HttpSinkConnectorConfig(config),executorService, getCompositeMeterRegistry());
            HttpResponse httpResponse = new HttpResponse(500, "Internal Server Error");
            boolean retryNeeded = configuration.retryNeeded(httpResponse);
            assertThat(retryNeeded).isTrue();
        }

        @Test
        public void test_retry_not_needed_with_400_status_code() {
            Map<String,String> config = Maps.newHashMap();
            config.put("httpclient.dummy."+RETRY_RESPONSE_CODE_REGEX,"^5[0-9][0-9]$");
            Configuration configuration = new Configuration("dummy",new HttpSinkConnectorConfig(config),executorService,getCompositeMeterRegistry());
            HttpResponse httpResponse = new HttpResponse(400, "Internal Server Error");
            boolean retryNeeded = configuration.retryNeeded(httpResponse);
            assertThat(retryNeeded).isFalse();
        }

        @Test
        public void test_retry_not_needed_with_200_status_code() {
            Map<String,String> config = Maps.newHashMap();
            config.put("httpclient.dummy."+RETRY_RESPONSE_CODE_REGEX,"^5[0-9][0-9]$");
            Configuration configuration = new Configuration("dummy",new HttpSinkConnectorConfig(config),executorService,getCompositeMeterRegistry());
            HttpResponse httpResponse = new HttpResponse(200, "Internal Server Error");
            boolean retryNeeded = configuration.retryNeeded(httpResponse);
            assertThat(retryNeeded).isFalse();
        }


        @Test
        public void test_retry_needed_by_configuration_with_200_status_code() {
            Map<String,String> config = Maps.newHashMap();
            config.put("config.dummy."+RETRY_RESPONSE_CODE_REGEX,"^2[0-9][0-9]$");
            config.put(CONFIG_DEFAULT_RETRY_RESPONSE_CODE_REGEX, "^[1-5][0-9][0-9]$");
            Configuration configuration = new Configuration("dummy",new HttpSinkConnectorConfig(config),executorService,getCompositeMeterRegistry());
            HttpResponse httpResponse = new HttpResponse(200, "Internal Server Error");

            boolean retryNeeded = configuration.retryNeeded(httpResponse);
            assertThat(retryNeeded).isTrue();
        }
    }

    private HttpExchange getDummyHttpExchange() {
        HttpRequest httpRequest = getDummyHttpRequest();
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