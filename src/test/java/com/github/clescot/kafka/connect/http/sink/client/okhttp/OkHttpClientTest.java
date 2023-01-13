package com.github.clescot.kafka.connect.http.sink.client.okhttp;

import com.github.clescot.kafka.connect.http.HttpRequest;
import com.github.clescot.kafka.connect.http.sink.client.HttpClient;
import com.google.common.collect.Maps;
import okhttp3.Request;
import okhttp3.RequestBody;
import okio.Buffer;
import okio.BufferedSink;
import okio.RealBufferedSink;
import okio.Sink;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class OkHttpClientTest {

    private Logger LOGGER = LoggerFactory.getLogger(OkHttpClientTest.class);
    @Test
    public void test_build_request() throws IOException {
        OkHttpClient client = new OkHttpClient(Maps.newHashMap());
        HttpRequest httpRequest = new HttpRequest("http://dummy.com/","GET",HttpRequest.BodyType.STRING.name());
        httpRequest.setBodyAsString("stuff");
        Request request = client.buildRequest(httpRequest);
        LOGGER.debug("request:{}",request);
        assertThat(request.url().url().toString()).isEqualTo(httpRequest.getUrl());
        assertThat(request.method()).isEqualTo(httpRequest.getMethod());
        RequestBody body = request.body();
        final Buffer buffer = new Buffer();
        body.writeTo(buffer);
        assertThat(buffer.readUtf8()).isEqualTo(httpRequest.getBodyAsString());
    }


}