package io.github.clescot.kafka.connect.http.sink;

import com.google.common.collect.Maps;
import dev.failsafe.RateLimiter;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigurationTest {

    @Nested
    class TestConstructor{
        @Test
        @DisplayName("test Configuration constructor with null parameters")
        public void test_constructor_with_null_parameters(){
            Assertions.assertThrows(NullPointerException.class,()->
            new Configuration(null,null));
        }
        @Test
        @DisplayName("test Configuration constructor with url predicate")
        public void test_constructor_with_url_predicate(){
            Map<String,String> settings = Maps.newHashMap();
            settings.put("httpclient.test.url.regex","^.*toto\\.com$");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration configuration = new Configuration("test", httpSinkConnectorConfig);
            HttpRequest httpRequest = new HttpRequest("http://toto.com","GET", HttpRequest.BodyType.STRING.name());
            assertThat(configuration.matches(httpRequest)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://titi.com","GET", HttpRequest.BodyType.STRING.name());
            assertThat(configuration.matches(httpRequest2)).isFalse();
        }

        @Test
        @DisplayName("test Configuration constructor with url predicate and method")
        public void test_constructor_with_url_predicate_and_method(){
            Map<String,String> settings = Maps.newHashMap();
            settings.put("httpclient.test.url.regex","^.*toto\\.com$");
            settings.put("httpclient.test.method.regex","^GET|PUT$");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration configuration = new Configuration("test", httpSinkConnectorConfig);
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
            settings.put("httpclient.test.url.regex","^.*toto\\.com$");
            settings.put("httpclient.test.bodytype.regex","^STRING$");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration configuration = new Configuration("test", httpSinkConnectorConfig);
            HttpRequest httpRequest1 = new HttpRequest("http://toto.com","GET", HttpRequest.BodyType.STRING.name());
            assertThat(configuration.matches(httpRequest1)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://toto.com","GET", HttpRequest.BodyType.FORM.name());
            assertThat(configuration.matches(httpRequest2)).isFalse();

        }

        @Test
        @DisplayName("test Configuration constructor with url predicate and header key")
        public void test_constructor_with_url_predicate_and_header_key(){
            Map<String,String> settings = Maps.newHashMap();
            settings.put("httpclient.test.url.regex","^.*toto\\.com$");
            settings.put("httpclient.test.header.key","SUPERNOVA");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration configuration = new Configuration("test", httpSinkConnectorConfig);
            HttpRequest httpRequest1 = new HttpRequest("http://toto.com","GET", HttpRequest.BodyType.STRING.name());
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("SUPERNOVA", List.of("stuff"));
            httpRequest1.setHeaders(headers);
            assertThat(configuration.matches(httpRequest1)).isTrue();
            HttpRequest httpRequest2 = new HttpRequest("http://toto.com","GET", HttpRequest.BodyType.FORM.name());
            assertThat(configuration.matches(httpRequest2)).isFalse();
        }

        @Test
        @DisplayName("test Configuration constructor with url predicate and header key")
        public void test_constructor_with_url_predicate_header_key_and_value(){
            Map<String,String> settings = Maps.newHashMap();
            settings.put("httpclient.test.url.regex","^.*toto\\.com$");
            settings.put("httpclient.test.header.key","SUPERNOVA");
            settings.put("httpclient.test.header.value","top");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration configuration = new Configuration("test", httpSinkConnectorConfig);
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
    }

    @Nested
    class TestStaticRatLimiter{

        @Test
        @DisplayName("test rate limiter with implicit instance scope")
        public void test_rate_limiter_with_implicit_instance_scope(){
            Map<String,String> settings = Maps.newHashMap();
            settings.put("httpclient.test.url.regex","^.*toto\\.com$");
            settings.put("httpclient.test.header.key","SUPERNOVA");
            settings.put("httpclient.test.header.value","top");
            settings.put("httpclient.test.rate.limiter.max.executions","3");
            settings.put("httpclient.test.rate.limiter.rate.limiter.period.in.ms","1000");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration configuration = new Configuration("test", httpSinkConnectorConfig);
            Optional<RateLimiter<HttpExchange>> rateLimiter = configuration.getRateLimiter();
            assertThat(rateLimiter.isPresent()).isTrue();
            Configuration configuration2 = new Configuration("test", httpSinkConnectorConfig);
            Optional<RateLimiter<HttpExchange>> rateLimiter2 = configuration2.getRateLimiter();
            assertThat(rateLimiter2.isPresent()).isTrue();
            assertThat(rateLimiter.get()!=rateLimiter2.get()).isTrue();
        }

        @Test
        @DisplayName("test rate limiter with static scope")
        public void test_rate_limiter_with_static_scope(){
            Map<String,String> settings = Maps.newHashMap();
            settings.put("httpclient.test.url.regex","^.*toto\\.com$");
            settings.put("httpclient.test.header.key","SUPERNOVA");
            settings.put("httpclient.test.header.value","top");
            settings.put("httpclient.test.rate.limiter.max.executions","3");
            settings.put("httpclient.test.rate.limiter.rate.limiter.period.in.ms","1000");
            settings.put("httpclient.test.rate.limiter.scope","static");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration configuration = new Configuration("test", httpSinkConnectorConfig);
            Optional<RateLimiter<HttpExchange>> rateLimiter = configuration.getRateLimiter();
            assertThat(rateLimiter.isPresent()).isTrue();
            Configuration configuration2 = new Configuration("test", httpSinkConnectorConfig);
            Optional<RateLimiter<HttpExchange>> rateLimiter2 = configuration2.getRateLimiter();
            assertThat(rateLimiter2.isPresent()).isTrue();
            assertThat(rateLimiter.get()==rateLimiter2.get()).isTrue();
        }
        @Test
        @DisplayName("test rate limiter with static scope")
        public void test_rate_limiter_with_static_scope_and_different_ids(){
            Map<String,String> settings = Maps.newHashMap();
            settings.put("httpclient.test.url.regex","^.*toto\\.com$");
            settings.put("httpclient.test.header.key","SUPERNOVA");
            settings.put("httpclient.test.header.value","top");
            settings.put("httpclient.test.rate.limiter.max.executions","3");
            settings.put("httpclient.test.rate.limiter.rate.limiter.period.in.ms","1000");
            settings.put("httpclient.test.rate.limiter.scope","static");
            HttpSinkConnectorConfig httpSinkConnectorConfig = new HttpSinkConnectorConfig(settings);
            Configuration configuration = new Configuration("test", httpSinkConnectorConfig);
            Optional<RateLimiter<HttpExchange>> rateLimiter = configuration.getRateLimiter();
            assertThat(rateLimiter.isPresent()).isTrue();
            Map<String,String> settings2 = Maps.newHashMap();
            settings2.put("httpclient.test2.url.regex","^.*toto\\.com$");
            settings2.put("httpclient.test2.header.key","SUPERNOVA");
            settings2.put("httpclient.test2.header.value","top");
            settings2.put("httpclient.test2.rate.limiter.max.executions","3");
            settings2.put("httpclient.test2.rate.limiter.rate.limiter.period.in.ms","1000");
            settings2.put("httpclient.test2.rate.limiter.scope","static");
            HttpSinkConnectorConfig httpSinkConnectorConfig2 = new HttpSinkConnectorConfig(settings2);
            Configuration configuration2 = new Configuration("test2", httpSinkConnectorConfig2);
            Optional<RateLimiter<HttpExchange>> rateLimiter2 = configuration2.getRateLimiter();
            assertThat(rateLimiter2.isPresent()).isTrue();
            assertThat(rateLimiter.get()!=rateLimiter2.get()).isTrue();
        }

    }

}