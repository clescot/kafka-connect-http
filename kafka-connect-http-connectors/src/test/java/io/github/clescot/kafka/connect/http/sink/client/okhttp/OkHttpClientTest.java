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
import io.github.clescot.kafka.connect.http.sink.client.proxy.URIRegexProxySelector;
import okhttp3.*;
import okhttp3.internal.http.RealResponseBody;
import okio.Buffer;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class OkHttpClientTest {

    public static final String UNAUTHORIZED = "unauthorized";
    public static final String ACCESS_GRANTED = "access granted";
    private final Logger LOGGER = LoggerFactory.getLogger(OkHttpClientTest.class);

    @RegisterExtension
    static WireMockExtension wmHttp;

    static {

        wmHttp = WireMockExtension.newInstance()
                .options(
                        WireMockConfiguration.wireMockConfig()
                                .dynamicPort()
//                                .networkTrafficListener(new ConsoleNotifyingWiremockNetworkTrafficListener())

                )
                .build();
    }

    @Nested
    class BuildRequest {
        @Test
        public void test_build_POST_request() throws IOException {

            //given
            OkHttpClient client = new OkHttpClient(Maps.newHashMap(), null,new Random(),null,null);
            HttpRequest httpRequest = new HttpRequest("http://dummy.com/", "POST", HttpRequest.BodyType.STRING.name());
            httpRequest.setBodyAsString("stuff");

            //given
            Request request = client.buildRequest(httpRequest);

            //then
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

            //given
            OkHttpClient client = new OkHttpClient(Maps.newHashMap(), null,new Random(),null,null);
            HttpRequest httpRequest = new HttpRequest("http://dummy.com/", "GET", HttpRequest.BodyType.STRING.name());
            httpRequest.setBodyAsString("stuff");

            //when
            Request request = client.buildRequest(httpRequest);

            //then
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

            //given
            OkHttpClient client = new OkHttpClient(Maps.newHashMap(), null,new Random(),null,null);

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

            //when
            HttpResponse httpResponse = client.buildResponse(response);

            //then
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
            OkHttpClient client = new OkHttpClient(config, null,new Random(),null,null);
        }

        @Test
        public void test_activated_cache_with_file_type_and_max_entries() {
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(OKHTTP_CACHE_ACTIVATE, "true");
            config.put(OKHTTP_CACHE_MAX_SIZE, "50000");
            OkHttpClient client = new OkHttpClient(config, null,new Random(),null,null);
        }

        @Test
        public void test_activated_cache_with_file_type_and_max_entries_and_location() {
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(OKHTTP_CACHE_ACTIVATE, "true");
            config.put(OKHTTP_CACHE_MAX_SIZE, "50000");
            config.put(OKHTTP_CACHE_DIRECTORY_PATH, "/tmp/toto");
            OkHttpClient client = new OkHttpClient(config, null,new Random(),null,null);
        }

        @Test
        public void test_activated_cache_with_inmemory_type() {
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(OKHTTP_CACHE_ACTIVATE, "true");
            config.put(OKHTTP_CACHE_TYPE, "inmemory");
            OkHttpClient client = new OkHttpClient(config, null,new Random(),null,null);
        }

        @Test
        public void test_inactivated_cache() {
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(OKHTTP_CACHE_ACTIVATE, "false");
            OkHttpClient client = new OkHttpClient(config, null,new Random(),null,null);
        }

        @Test
        public void test_no_cache() {
            HashMap<String, Object> config = Maps.newHashMap();
            OkHttpClient client = new OkHttpClient(config, null,new Random(),null,null);
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
            config.put("httpclient.authentication.basic.activate", true);
            config.put("httpclient.authentication.basic.username", username);
            config.put("httpclient.authentication.basic.password", password);

            OkHttpClient client = new OkHttpClient(config, null,new Random(),null,null);

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


            String scenario = "Basic Authentication";
            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs(STARTED)
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Date", "Wed, 21 Oct 2022 05:21:23 GMT")
                                    .withHeader("WWW-Authenticate", "Basic realm=\"Access to staging site\"")
                                    .withStatus(401)
                                    .withStatusMessage("Unauthorized")
                            ).willSetStateTo(UNAUTHORIZED)
                    );
            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs(UNAUTHORIZED)
                            .withBasicAuth(username, password)
                            .withHeader("Content-Type", containing("text/plain"))
                            .withHeader("X-Correlation-ID", containing("e6de70d1-f222-46e8-b755-754880687822"))
                            .withHeader("X-Request-ID", containing("e6de70d1-f222-46e8-b755-11111"))
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(ACCESS_GRANTED)
                    );
            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs(ACCESS_GRANTED)
                            .withBasicAuth(username, password)
                            .withHeader("Content-Type", containing("text/plain"))
                            .withHeader("X-Correlation-ID", containing("e6de70d1-f222-46e8-b755-754880687822"))
                            .withHeader("X-Request-ID", containing("e6de70d1-f222-46e8-b755-11111"))
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(ACCESS_GRANTED)
                    );


            HttpExchange httpExchange1 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange1.getHttpResponse().getStatusCode()).isEqualTo(200);
            HttpExchange httpExchange2 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange2.getHttpResponse().getStatusCode()).isEqualTo(200);

        }

        @Test
        @DisplayName("test Digest Authentication : two calls")
        void test_digest_authentication() throws ExecutionException, InterruptedException {

            String username = "user1";
            String password = "password1";
            String bodyResponse = "{\"result\":\"pong\"}";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();

            HashMap<String, Object> config = Maps.newHashMap();
            config.put("httpclient.authentication.digest.activate", true);
            config.put("httpclient.authentication.digest.username", username);
            config.put("httpclient.authentication.digest.password", password);
            Random random = mock(Random.class);
            byte[] randomBytes = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07};
            doAnswer(invocation -> {
                Object[] args = invocation.getArguments();
                byte[] rnd = (byte[]) args[0];
                System.arraycopy(randomBytes, 0, rnd, 0, randomBytes.length);
                return null;
            })
                    .when(random).nextBytes(any(byte[].class));
            OkHttpClient client = new OkHttpClient(config, null, random,null,null);

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

            String url2 = baseUrl + "/ping2";
            HashMap<String, List<String>> headers2 = Maps.newHashMap();
            headers2.put("Content-Type", Lists.newArrayList("text/plain"));
            headers2.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
            headers2.put("X-Request-ID", Lists.newArrayList("22222-33333-000-000-0000"));
            HttpRequest httpRequest2 = new HttpRequest(
                    url2,
                    "POST",
                    "STRING"
            );
            httpRequest2.setHeaders(headers2);
            httpRequest2.setBodyAsString("stuff2");

            String scenario = "Digest Authentication";
            wireMock
                    .register(WireMock.post("/ping")
                            .withName("1")
                            .inScenario(scenario)
                            .whenScenarioStateIs(STARTED)
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Date", "Wed, 21 Oct 2022 05:21:23 GMT")
                                    .withHeader("WWW-Authenticate",
                                            "Digest " +
                                                    "realm=\"Access to staging site\"," +
                                                    "qop=\"auth,auth-int\"," +
                                                    "nonce=\"dcd98b7102dd2f0e8b11d0f615bfb0c093\"," +
                                                    "opaque=\"5cdc029c403ebaf9f0171e9517f40e41\"")
                                    .withStatus(401)
                                    .withStatusMessage("Unauthorized")
                            ).willSetStateTo(UNAUTHORIZED)
                    );

            wireMock
                    .register(WireMock.post("/ping")
                            .withName("2")
                            .inScenario(scenario)
                            .whenScenarioStateIs(UNAUTHORIZED)
                            .withHeader("Authorization",
                                    equalTo("Digest " +
                                            "username=\"user1\", " +
                                            "realm=\"Access to staging site\", " +
                                            "nonce=\"dcd98b7102dd2f0e8b11d0f615bfb0c093\", " +
                                            "uri=\"/ping\", " +
                                            "response=\"15615fe29619fb8d064b7d8d49ef273a\", " +
                                            "qop=auth, " +
                                            "nc=00000001, " +
                                            "cnonce=\"0001020304050607\", " +
                                            "algorithm=MD5, " +
                                            "opaque=\"5cdc029c403ebaf9f0171e9517f40e41\""
                                    )
                            )
                            .withHeader("Content-Type", containing("text/plain"))
                            .withHeader("X-Correlation-ID", equalTo("e6de70d1-f222-46e8-b755-754880687822"))
                            .withHeader("X-Request-ID", equalTo("e6de70d1-f222-46e8-b755-11111"))
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(ACCESS_GRANTED)
                    );

            wireMock
                    .register(WireMock.post("/ping2")
                            .withName("3")
                            .inScenario(scenario)
                            .whenScenarioStateIs(ACCESS_GRANTED)
                            .withHeader("Authorization",
                                    equalTo("Digest " +
                                            "username=\"user1\", " +
                                            "realm=\"Access to staging site\", " +
                                            "nonce=\"dcd98b7102dd2f0e8b11d0f615bfb0c093\", " +
                                            "uri=\"/ping2\", " +
                                            "response=\"2b0343d234053807ee3c9d478d0f0874\", " +
                                            "qop=auth, " +
                                            "nc=00000002, " +
                                            "cnonce=\"0001020304050607\", " +
                                            "algorithm=MD5, " +
                                            "opaque=\"5cdc029c403ebaf9f0171e9517f40e41\""
                                    )
                            )
                            .withHeader("Content-Type", containing("text/plain"))
                            .withHeader("X-Correlation-ID", equalTo("e6de70d1-f222-46e8-b755-754880687822"))
                            .withHeader("X-Request-ID", equalTo("22222-33333-000-000-0000"))
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(ACCESS_GRANTED)
                    );


            HttpExchange httpExchange1 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange1.getHttpResponse().getStatusCode()).isEqualTo(200);

            HttpExchange httpExchange2 = client.call(httpRequest2, new AtomicInteger(1)).get();
            assertThat(httpExchange2.getHttpResponse().getStatusCode()).isEqualTo(200);

        }
    }

    private String getIP() {
        try (DatagramSocket datagramSocket = new DatagramSocket()) {
            datagramSocket.connect(InetAddress.getByName("8.8.8.8"), 12345);
            return datagramSocket.getLocalAddress().getHostAddress();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Nested
    class TestProxy {
        @Test
        @DisplayName("test proxy without authentication")
        void test_proxy_without_authentication() throws ExecutionException, InterruptedException {


            String bodyResponse = "{\"result\":\"pong\"}";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            //the test will call the proxy to try to forward the request, but wiremock won't relay.
            String baseUrl = "http://" + "dummy.com" + ":22222";
            String url = baseUrl + "/ping";

            HashMap<String, Object> config = Maps.newHashMap();
            Proxy proxy = new Proxy(Proxy.Type.HTTP,new InetSocketAddress(getIP(),wmRuntimeInfo.getHttpPort()));


            OkHttpClient client = new OkHttpClient(config, null,new Random(),proxy,null);

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


            String scenario = "Proxy";

            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs(STARTED)
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo("Started")
                    );

            HttpExchange httpExchange1 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange1.getHttpResponse().getStatusCode()).isEqualTo(200);
            HttpExchange httpExchange2 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange2.getHttpResponse().getStatusCode()).isEqualTo(200);

        }


        @Test
        @DisplayName("test proxy with basic authentication")
        void test_proxy_with_basic_authentication() throws ExecutionException, InterruptedException {


            String bodyResponse = "{\"result\":\"pong\"}";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            //the test will call the proxy to try to forward the request, but wiremock won't relay.
            String baseUrl = "http://" + "dummy.com" + ":22222";
            String url = baseUrl + "/ping";

            HashMap<String, Object> config = Maps.newHashMap();
            config.put(HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_ACTIVATE, true);
            String username = "user1";
            config.put(HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_USERNAME, username);
            String password = "password1";
            config.put(HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_PASSWORD, password);
            Proxy proxy = new Proxy(Proxy.Type.HTTP,new InetSocketAddress(getIP(),wmRuntimeInfo.getHttpPort()));

            OkHttpClient client = new OkHttpClient(config, null,new Random(),proxy,null);

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


            String scenario = "Proxy with authentication";

            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs(STARTED)
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Date", "Wed, 21 Oct 2022 05:21:23 GMT")
                                    .withHeader("Proxy-Authenticate", "Basic realm=\"Access to staging site\"")
                                    .withStatus(407)
                                    .withStatusMessage("Proxy Authentication Required")
                            ).willSetStateTo("Proxy Authentication Required")
                    );
            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs("Proxy Authentication Required")
                            .withHeader("Proxy-Authorization", containing("Basic dXNlcjE6cGFzc3dvcmQx"))
                            .withHeader("Content-Type", containing("text/plain"))
                            .withHeader("X-Correlation-ID", containing("e6de70d1-f222-46e8-b755-754880687822"))
                            .withHeader("X-Request-ID", containing("e6de70d1-f222-46e8-b755-11111"))
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(ACCESS_GRANTED)
                    );
            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs(ACCESS_GRANTED)
                            .withHeader("Content-Type", containing("text/plain"))
                            .withHeader("X-Correlation-ID", containing("e6de70d1-f222-46e8-b755-754880687822"))
                            .withHeader("X-Request-ID", containing("e6de70d1-f222-46e8-b755-11111"))
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(ACCESS_GRANTED)
                    );

            HttpExchange httpExchange1 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange1.getHttpResponse().getStatusCode()).isEqualTo(200);
            HttpExchange httpExchange2 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange2.getHttpResponse().getStatusCode()).isEqualTo(200);

        }

        @Test
        @DisplayName("test proxy with basic authentication and basic authentication on website")
        void test_proxy_with_basic_authentication_and_basic_authentication_on_website() throws ExecutionException, InterruptedException {


            String bodyResponse = "{\"result\":\"pong\"}";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            //the test will call the proxy to try to forward the request, but wiremock won't relay.
            String baseUrl = "http://" + getIP() + ":" + wmHttp.getPort();
            String url = baseUrl + "/ping";

            HashMap<String, Object> config = Maps.newHashMap();

            config.put(HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_ACTIVATE, true);
            String proxyUsername = "proxyuser1";
            config.put(HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_USERNAME, proxyUsername);
            String proxyPassword = "proxypassword1";
            config.put(HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_PASSWORD, proxyPassword);

            config.put("httpclient.authentication.basic.activate", true);
            String username = "user1";
            String password = "password1";
            config.put("httpclient.authentication.basic.username", username);
            config.put("httpclient.authentication.basic.password", password);
            Proxy proxy = new Proxy(Proxy.Type.HTTP,new InetSocketAddress(getIP(),wmRuntimeInfo.getHttpPort()));

            OkHttpClient client = new OkHttpClient(config, null,new Random(),proxy,null);

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


            String scenario = "Proxy with authentication";

            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs(STARTED)
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Date", "Wed, 21 Oct 2022 05:21:23 GMT")
                                    .withHeader("Proxy-Authenticate", "Basic realm=\"Access to staging site\"")
                                    .withStatus(407)
                                    .withStatusMessage("Proxy Authentication Required")
                            ).willSetStateTo("Proxy Authentication Required")
                    );
            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs("Proxy Authentication Required")
                            .withHeader("Proxy-Authorization", containing("Basic cHJveHl1c2VyMTpwcm94eXBhc3N3b3JkMQ=="))
                            .withHeader("Content-Type", containing("text/plain"))
                            .withHeader("X-Correlation-ID", containing("e6de70d1-f222-46e8-b755-754880687822"))
                            .withHeader("X-Request-ID", containing("e6de70d1-f222-46e8-b755-11111"))
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Date", "Wed, 21 Oct 2022 05:21:23 GMT")
                                    .withHeader("WWW-Authenticate", "Basic realm=\"Access to staging site\"")
                                    .withStatus(401)
                                    .withStatusMessage("Unauthorized")
                            ).willSetStateTo("Unauthorized")
                    );
            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs("Unauthorized")
                            .withHeader("Proxy-Authorization", containing("Basic cHJveHl1c2VyMTpwcm94eXBhc3N3b3JkMQ=="))
                            .withBasicAuth(username, password)
                            .withHeader("Content-Type", containing("text/plain"))
                            .withHeader("X-Correlation-ID", containing("e6de70d1-f222-46e8-b755-754880687822"))
                            .withHeader("X-Request-ID", containing("e6de70d1-f222-46e8-b755-11111"))
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(ACCESS_GRANTED)
                    );
            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs(ACCESS_GRANTED)
                            .withHeader("Proxy-Authorization", containing("Basic cHJveHl1c2VyMTpwcm94eXBhc3N3b3JkMQ=="))
                            .withBasicAuth(username, password)
                            .withHeader("Content-Type", containing("text/plain"))
                            .withHeader("X-Correlation-ID", containing("e6de70d1-f222-46e8-b755-754880687822"))
                            .withHeader("X-Request-ID", containing("e6de70d1-f222-46e8-b755-11111"))
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(ACCESS_GRANTED)
                    );

            HttpExchange httpExchange1 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange1.getHttpResponse().getStatusCode()).isEqualTo(200);
            HttpExchange httpExchange2 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange2.getHttpResponse().getStatusCode()).isEqualTo(200);

        }

    }

    @Nested
    class TestProxySelector {
        @Test
        @DisplayName("test proxy selector without authentication with one proxy")
        void test_proxy_selector_without_authentication_one_proxy() throws ExecutionException, InterruptedException {
            String bodyResponse = "{\"result\":\"pong\"}";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            //the test will call the proxy to try to forward the request, but wiremock won't relay.
            String baseUrl = "http://" + "dummy.com" + ":22222";
            String url = baseUrl + "/ping";

            HashMap<String, Object> config = Maps.newHashMap();

            List<ImmutablePair<Predicate<URI>,Proxy>> proxies = Lists.newArrayList();
            Pattern uriPattern = Pattern.compile(".*");
            Predicate<URI> predicate = uri -> uriPattern.matcher(uri.toString()).matches();
            Proxy proxy = new Proxy(Proxy.Type.HTTP,new InetSocketAddress(getIP(),wmRuntimeInfo.getHttpPort()));
            ImmutablePair<Predicate<URI>,Proxy> pair = new ImmutablePair(predicate,proxy);
            proxies.add(pair);
            URIRegexProxySelector proxySelector = new URIRegexProxySelector(proxies);
            OkHttpClient client = new OkHttpClient(config, null,new Random(),null,proxySelector);

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


            String scenario = "Proxy";

            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs(STARTED)
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo("Started")
                    );

            HttpExchange httpExchange1 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange1.getHttpResponse().getStatusCode()).isEqualTo(200);
            HttpExchange httpExchange2 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange2.getHttpResponse().getStatusCode()).isEqualTo(200);
        }  @Test
        @DisplayName("test proxy selector without authentication with two proxies")
        void test_proxy_selector_without_authentication_two_proxies() throws ExecutionException, InterruptedException {
            String bodyResponse = "{\"result\":\"pong\"}";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            //the test will call the proxy to try to forward the request, but wiremock won't relay.
            String baseUrl = "http://" + "dummy.com" + ":22222";
            String url = baseUrl + "/ping";

            HashMap<String, Object> config = Maps.newHashMap();


            List<ImmutablePair<Predicate<URI>,Proxy>> proxies = Lists.newArrayList();

            Pattern uriPattern = Pattern.compile("http://toto\\.com.*");
            Predicate<URI> predicate = uri -> uriPattern.matcher(uri.toString()).matches();
            Proxy proxy = new Proxy(Proxy.Type.HTTP,new InetSocketAddress("111.222.888.999",5555));
            ImmutablePair<Predicate<URI>,Proxy> pair = new ImmutablePair(predicate,proxy);
            proxies.add(pair);
            Pattern uriPattern2 = Pattern.compile("http://dummy\\.com.*");
            Predicate<URI> predicate2 = uri -> uriPattern2.matcher(uri.toString()).matches();
            Proxy proxy2 = new Proxy(Proxy.Type.HTTP,new InetSocketAddress(getIP(),wmRuntimeInfo.getHttpPort()));
            ImmutablePair<Predicate<URI>,Proxy> pair2 = new ImmutablePair(predicate2,proxy2);
            proxies.add(pair2);

            URIRegexProxySelector proxySelector = new URIRegexProxySelector(proxies);

            OkHttpClient client = new OkHttpClient(config, null,new Random(),null,proxySelector);

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


            String scenario = "Proxy";

            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs(STARTED)
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo("Started")
                    );

            HttpExchange httpExchange1 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange1.getHttpResponse().getStatusCode()).isEqualTo(200);
            HttpExchange httpExchange2 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange2.getHttpResponse().getStatusCode()).isEqualTo(200);
        }

        @AfterEach
        public void afterEach() {
            wmHttp.resetAll();
            QueueFactory.clearRegistrations();
        }
    }
}
