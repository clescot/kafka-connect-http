package io.github.clescot.kafka.connect.http.core;


import com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class HttpResponseBuilderTest {

    @Test
    void test_negative_limits(){
        HttpResponseBuilder httpResponseBuilder = new HttpResponseBuilder(-100,-100,-100);
        httpResponseBuilder.setStatus(200,"1234");
        httpResponseBuilder.setBodyAsString("abcd");
        assertThat(httpResponseBuilder.getStatusMessageLimit()).isEqualTo(0);
        assertThat(httpResponseBuilder.getBodyLimit()).isEqualTo(0);
        HttpResponse httpResponse = httpResponseBuilder.toHttpResponse();
        assertThat(httpResponse.getStatusMessage()).isEmpty();
        assertThat(httpResponse.getBodyAsString()).isEmpty();
    }

    @Test
    void test_limits_set_to_zero(){
        HttpResponseBuilder httpResponseBuilder = new HttpResponseBuilder(0,0,0);
        httpResponseBuilder.setStatus(200,"1234");
        httpResponseBuilder.setBodyAsString("abcd");
        assertThat(httpResponseBuilder.getStatusMessageLimit()).isEqualTo(0);
        assertThat(httpResponseBuilder.getBodyLimit()).isEqualTo(0);
        HttpResponse httpResponse = httpResponseBuilder.toHttpResponse();
        assertThat(httpResponse.getStatusMessage()).isEmpty();
        assertThat(httpResponse.getBodyAsString()).isEmpty();
    }

    @Test
    void test_positive_limits(){
        HttpResponseBuilder httpResponseBuilder = new HttpResponseBuilder(2,10,2);
        httpResponseBuilder.setStatus(200,"1234");
        httpResponseBuilder.setBodyAsString("abcd");
        assertThat(httpResponseBuilder.getStatusMessageLimit()).isEqualTo(2);
        assertThat(httpResponseBuilder.getBodyLimit()).isEqualTo(2);
        HttpResponse httpResponse = httpResponseBuilder.toHttpResponse();
        assertThat(httpResponse.getStatusMessage()).isEqualTo("12");
        assertThat(httpResponse.getBodyAsString()).isEqualTo("ab");
    }
    @Test
    void test_minus_one_limits(){
        HttpResponseBuilder httpResponseBuilder = new HttpResponseBuilder(-1,-1,-1);
        httpResponseBuilder.setStatus(200,"1234");
        httpResponseBuilder.setBodyAsString("abcd");
        assertThat(httpResponseBuilder.getStatusMessageLimit()).isEqualTo(0);
        assertThat(httpResponseBuilder.getBodyLimit()).isEqualTo(0);
        HttpResponse httpResponse = httpResponseBuilder.toHttpResponse();
        assertThat(httpResponse.getStatusMessage()).isEmpty();
        assertThat(httpResponse.getBodyAsString()).isEmpty();
    }
    @Test
    void test_minus_one_limits_and_status_and_body_are_null(){
        HttpResponseBuilder httpResponseBuilder = new HttpResponseBuilder(-1,-1,-1);
        httpResponseBuilder.setStatus(200,null);
        httpResponseBuilder.setBodyAsString(null);
        assertThat(httpResponseBuilder.getStatusMessageLimit()).isEqualTo(0);
        assertThat(httpResponseBuilder.getBodyLimit()).isEqualTo(0);
        HttpResponse httpResponse = httpResponseBuilder.toHttpResponse();
        assertThat(httpResponse.getStatusMessage()).isNull();
        assertThat(httpResponse.getBodyAsString()).isNull();
    }

    @Test
    void test_status_message_set_to_null(){
        HttpResponseBuilder httpResponseBuilder = new HttpResponseBuilder(2,2,2);
        httpResponseBuilder.setStatus(200,null);
        httpResponseBuilder.setBodyAsString("abcd");
        assertThat(httpResponseBuilder.getStatusMessageLimit()).isEqualTo(2);
        assertThat(httpResponseBuilder.getBodyLimit()).isEqualTo(2);
        HttpResponse httpResponse = httpResponseBuilder.toHttpResponse();
        assertThat(httpResponse.getStatusMessage()).isEqualTo(null);
        assertThat(httpResponse.getBodyAsString()).isEqualTo("ab");
    }

    @Test
    void test_body_set_to_null(){
        HttpResponseBuilder httpResponseBuilder = new HttpResponseBuilder(10,10,10);
        httpResponseBuilder.setStatus(200,null);
        httpResponseBuilder.setBodyAsString(null);
        assertThat(httpResponseBuilder.getStatusMessageLimit()).isEqualTo(10);
        assertThat(httpResponseBuilder.getBodyLimit()).isEqualTo(10);
        HttpResponse httpResponse = httpResponseBuilder.toHttpResponse();
        assertThat(httpResponse.getStatusMessage()).isEqualTo(null);
        assertThat(httpResponse.getBodyAsString()).isEqualTo(null);
    }


    @Test
    void test_nominal_case(){
        int statusMessageLimit = 250;
        int headersLimit = 800;
        int bodyLimit = 2000;
        HttpResponseBuilder httpResponseBuilder = new HttpResponseBuilder(statusMessageLimit, headersLimit, bodyLimit);
        String statusMessage = "1234";
        int statusCode = 200;
        httpResponseBuilder.setStatus(statusCode, statusMessage);
        String protocol = "http";
        httpResponseBuilder.setProtocol(protocol);
        Map<String, List<String>> headers = Maps.newHashMap();
        headers.put("key1", List.of("value1"));
        headers.put("key2", List.of("value1","value2"));
        httpResponseBuilder.setHeaders(headers);
        String bodyAsString = "abcd";
        httpResponseBuilder.setBodyAsString(bodyAsString);
        assertThat(httpResponseBuilder.getStatusMessageLimit()).isEqualTo(statusMessageLimit);
        assertThat(httpResponseBuilder.getBodyLimit()).isEqualTo(bodyLimit);
        HttpResponse httpResponse = httpResponseBuilder.toHttpResponse();
        assertThat(httpResponse.getHeaders()).containsAllEntriesOf(headers);
        assertThat(httpResponse.getStatusCode()).isEqualTo(statusCode);
        assertThat(httpResponse.getStatusMessage()).isEqualTo(statusMessage);
        assertThat(httpResponse.getProtocol()).isEqualTo(protocol);
        assertThat(httpResponse.getBodyAsString()).isEqualTo(bodyAsString);
    }
}