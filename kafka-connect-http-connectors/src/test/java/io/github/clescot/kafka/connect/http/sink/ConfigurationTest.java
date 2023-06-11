package io.github.clescot.kafka.connect.http.sink;

import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.apache.commons.compress.utils.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigurationTest {

    @Nested
    class constructor{
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
    }

}