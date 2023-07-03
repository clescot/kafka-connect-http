package io.github.clescot.kafka.connect.http.sink.client.okhttp;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import okhttp3.*;
import okhttp3.internal.http.RealResponseBody;
import okio.Buffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient.*;
import static org.assertj.core.api.Assertions.assertThat;

class OkHttpClientTest {

    private final Logger LOGGER = LoggerFactory.getLogger(OkHttpClientTest.class);

    @RegisterExtension
    static WireMockExtension wmHttp = WireMockExtension.newInstance()
            .options(
                    WireMockConfiguration.wireMockConfig()
                            .dynamicPort()
            )
            .build();

    @Nested
    class BuildRequest {
        @Test
        public void test_build_POST_request() throws IOException {
            io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient(Maps.newHashMap(), null);
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
            io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient(Maps.newHashMap(), null);
            HttpRequest httpRequest = new HttpRequest("http://dummy.com/", "GET", HttpRequest.BodyType.STRING.name());
            httpRequest.setBodyAsString("stuff");
            Request request = client.buildRequest(httpRequest);
            LOGGER.debug("request:{}", request);
            assertThat(request.url().url().toString()).isEqualTo(httpRequest.getUrl());
            assertThat(request.method()).isEqualTo(httpRequest.getMethod());
            assertThat(request.body()).isNull();
        }

    }

    @Nested
    class BuildResponse {
        @Test
        public void test_build_response() {
            io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient client = new OkHttpClient(Maps.newHashMap(), null);

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

    @Nested
    class Cache {
        @Test
        public void test_activated_cache_with_file_type() {
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(OKHTTP_CACHE_ACTIVATE, "true");
            io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient(config, null);
        }

        @Test
        public void test_activated_cache_with_file_type_and_max_entries() {
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(OKHTTP_CACHE_ACTIVATE, "true");
            config.put(OKHTTP_CACHE_MAX_SIZE, "50000");
            io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient(config, null);
        }

        @Test
        public void test_activated_cache_with_file_type_and_max_entries_and_location() {
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(OKHTTP_CACHE_ACTIVATE, "true");
            config.put(OKHTTP_CACHE_MAX_SIZE, "50000");
            config.put(OKHTTP_CACHE_DIRECTORY_PATH, "/tmp/toto");
            io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient(config, null);
        }

        @Test
        public void test_activated_cache_with_inmemory_type() {
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(OKHTTP_CACHE_ACTIVATE, "true");
            config.put(OKHTTP_CACHE_TYPE, "inmemory");
            io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient(config, null);
        }

        @Test
        public void test_inactivated_cache() {
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(OKHTTP_CACHE_ACTIVATE, "false");
            io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient(config, null);
        }

        @Test
        public void test_no_cache() {
            HashMap<String, Object> config = Maps.newHashMap();
            io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClient(config, null);
        }
    }

    @Nested
    class TestAuthentication {

        @Test
        @DisplayName("test Basic Authentication : two calls")
        public void test_basic_authentication() throws ExecutionException, InterruptedException {

            String username = "user1";
            String password = "password1";
            String bodyResponse = "{\"result\":\"pong\"}";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();

            HashMap<String, Object> config = Maps.newHashMap();
            config.put("httpclient.authentication.basic.activate",true);
            config.put("httpclient.authentication.basic.username",username);
            config.put("httpclient.authentication.basic.password",password);

            OkHttpClient client = new OkHttpClient(config, null);

            String baseUrl = "http://" + getIP() + ":" + wmRuntimeInfo.getHttpPort();
            String url = baseUrl + "/ping";
            HashMap<String, List<String>> headers = Maps.newHashMap();
            headers.put("Content-Type", Lists.newArrayList("text/plain"));
            headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
            headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
            HttpRequest httpRequest = new HttpRequest(
                    url,
                    "POST",
                    "STRING"
            );
            httpRequest.setHeaders(headers);
            httpRequest.setBodyAsString("stuff");


            wireMock
                    .register(WireMock.post("/ping").inScenario("Basic Authentication")
                            .whenScenarioStateIs(STARTED)
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Date","Wed, 21 Oct 2022 05:21:23 GMT")
                                    .withHeader("WWW-Authenticate","Basic realm=\"Access to staging site\"")
                                    .withBody(bodyResponse)
                                    .withStatus(401)
                                    .withStatusMessage("Unauthorized")
                            ).willSetStateTo("unauthorized")
                    );
            wireMock
                    .register(WireMock.post("/ping").inScenario("Basic Authentication")
                            .whenScenarioStateIs("unauthorized")
                            .withBasicAuth(username,password)
                            .withHeader("Content-Type", containing("text/plain"))
                            .withHeader("X-Correlation-ID", containing("e6de70d1-f222-46e8-b755-754880687822"))
                            .withHeader("X-Request-ID", containing("e6de70d1-f222-46e8-b755-11111"))
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo("access granted")
                    );
            wireMock
                    .register(WireMock.post("/ping").inScenario("Basic Authentication")
                            .whenScenarioStateIs("access granted")
                            .withBasicAuth(username,password)
                            .withHeader("Content-Type", containing("text/plain"))
                            .withHeader("X-Correlation-ID", containing("e6de70d1-f222-46e8-b755-754880687822"))
                            .withHeader("X-Request-ID", containing("e6de70d1-f222-46e8-b755-11111"))
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo("access granted")
                    );


            HttpExchange httpExchange1 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange1.getHttpResponse().getStatusCode()).isEqualTo(200);
            HttpExchange httpExchange2 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange2.getHttpResponse().getStatusCode()).isEqualTo(200);

        }

        private String getIP() {
            try (DatagramSocket datagramSocket = new DatagramSocket()) {
                datagramSocket.connect(InetAddress.getByName("8.8.8.8"), 12345);
                return datagramSocket.getLocalAddress().getHostAddress();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }


        @AfterEach
        public void afterEach() {
            wmHttp.resetAll();
            QueueFactory.clearRegistrations();
        }
    }


}