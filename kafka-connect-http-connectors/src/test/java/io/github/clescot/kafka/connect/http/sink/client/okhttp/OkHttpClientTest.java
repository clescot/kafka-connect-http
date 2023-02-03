package io.github.clescot.kafka.connect.http.sink.client.okhttp;

import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import com.google.common.collect.Maps;
import okhttp3.*;
import okhttp3.internal.http.RealResponseBody;
import okio.Buffer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import static org.assertj.core.api.Assertions.*;

class OkHttpClientTest {

    private Logger LOGGER = LoggerFactory.getLogger(OkHttpClientTest.class);

    @Test
    public void test_build_POST_request() throws IOException {
        io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient(Maps.newHashMap());
        HttpRequest httpRequest = new HttpRequest("http://dummy.com/", "POST", HttpRequest.BodyType.STRING.name());
        httpRequest.setBodyAsString("stuff");
        Request request = client.buildRequest(httpRequest);
        LOGGER.debug("request:{}", request);
        assertThat(request.url().url().toString()).isEqualTo(httpRequest.getUrl());
        assertThat(request.method()).isEqualTo(httpRequest.getMethod());
        RequestBody body = request.body();
        final Buffer buffer = new Buffer();
        body.writeTo(buffer);
        assertThat(buffer.readUtf8()).isEqualTo(httpRequest.getBodyAsString());
    }


    @Test
    public void test_build_GET_request_with_body() {
        io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient(Maps.newHashMap());
        HttpRequest httpRequest = new HttpRequest("http://dummy.com/", "GET", HttpRequest.BodyType.STRING.name());
        httpRequest.setBodyAsString("stuff");
        Request request = client.buildRequest(httpRequest);
        LOGGER.debug("request:{}", request);
        assertThat(request.url().url().toString()).isEqualTo(httpRequest.getUrl());
        assertThat(request.method()).isEqualTo(httpRequest.getMethod());
        assertThat(request.body()).isNull();
    }

    @Test
    public void test_build_response() {
        io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient client = new OkHttpClient(Maps.newHashMap());

        HttpRequest httpRequest = new HttpRequest("http://dummy.com/", "POST", HttpRequest.BodyType.STRING.name());
        httpRequest.setBodyAsString("stuff");
        Request request = client.buildRequest(httpRequest);

        Response.Builder builder = new Response.Builder();
        Headers headers = new Headers.Builder()
                .add("key1", "value1")
                .add("Content-Type", "application/json")
                .build();
        builder.headers(headers);
        builder.request(request);
        builder.code(200);
        builder.message("OK");
        String responseContent = "blabla";
        Buffer buffer = new Buffer();
        buffer.write(responseContent.getBytes(StandardCharsets.UTF_8));
        ResponseBody responseBody = new RealResponseBody("application/json", responseContent.length(), buffer);
        builder.body(responseBody);
        builder.protocol(Protocol.HTTP_1_1);
        Response response = builder.build();
        HttpResponse httpResponse = client.buildResponse(response);
        LOGGER.debug("response:{}", response);
        assertThat(response.code()).isEqualTo(httpResponse.getStatusCode());
        assertThat(response.message()).isEqualTo(httpResponse.getStatusMessage());
        assertThat(response.header("key1")).isEqualTo(httpResponse.getResponseHeaders().get("key1").get(0));
        assertThat(response.header("Content-Type")).isEqualTo(httpResponse.getResponseHeaders().get("Content-Type").get(0));

    }


}