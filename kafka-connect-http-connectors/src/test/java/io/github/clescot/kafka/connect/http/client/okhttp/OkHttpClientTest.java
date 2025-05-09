package io.github.clescot.kafka.connect.http.client.okhttp;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import io.github.clescot.kafka.connect.http.client.DummyX509Certificate;
import io.github.clescot.kafka.connect.http.client.HttpClient;
import io.github.clescot.kafka.connect.http.client.proxy.URIRegexProxySelector;
import io.github.clescot.kafka.connect.http.core.*;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.jmx.JmxMeterRegistry;
import okhttp3.*;
import okhttp3.internal.http.RealResponseBody;
import okio.Buffer;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.assertj.core.util.Sets;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static io.github.clescot.kafka.connect.http.client.Configuration.CONFIGURATION_ID;
import static io.github.clescot.kafka.connect.http.client.HttpClient.THROWABLE_CLASS;
import static io.github.clescot.kafka.connect.http.client.HttpClient.THROWABLE_MESSAGE;
import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class OkHttpClientTest {
    public static final String ACCESS_GRANTED_STATE = "access_granted";
    public static final String UNAUTHORIZED_STATE = "Unauthorized";
    public static final String PROXY_AUTHENTICATION_REQUIRED_STATE = "Proxy_Authentication_Required";
    public static final String CONTENT_TYPE = "Content-Type";
    public static final String APPLICATION_JSON = "application/json";

    private final Logger LOGGER = LoggerFactory.getLogger(OkHttpClientTest.class);

    @RegisterExtension
    static WireMockExtension wmHttp;

    static {

        wmHttp = WireMockExtension.newInstance()
                .options(
                        WireMockConfiguration.wireMockConfig()
                                .dynamicPort()
                                .dynamicHttpsPort()
                                .keystorePath(Resources.getResource("test.jks").toString())
                                .keystorePassword("password")
                                .keystoreType("JKS")
//                                .networkTrafficListener(new ConsoleNotifyingWiremockNetworkTrafficListener())
                                .useChunkedTransferEncoding(Options.ChunkedEncodingPolicy.NEVER)
                )
                .build();
    }


    @NotNull
    private CompositeMeterRegistry getCompositeMeterRegistry() {
        JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry(s -> null, Clock.SYSTEM);
        HashSet<MeterRegistry> registries = Sets.newHashSet();
        registries.add(jmxMeterRegistry);
        return new CompositeMeterRegistry(Clock.SYSTEM, registries);
    }

    @Nested
    class BuildRequest {
        @Test
        void test_build_POST_request_with_body_as_string() throws IOException {

            //given
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());
            HttpRequest httpRequest = new HttpRequest("http://dummy.com/", HttpRequest.Method.POST);
            httpRequest.setBodyAsString("stuff");

            //given
            Request request = client.buildRequest(httpRequest);

            //then
            LOGGER.debug("request:{}", request);
            assertThat(request.url().url().toString()).hasToString(httpRequest.getUrl());
            assertThat(request.method()).isEqualTo(httpRequest.getMethod().name());
            RequestBody body = request.body();
            final Buffer buffer = new Buffer();
            body.writeTo(buffer);
            assertThat(buffer.readUtf8()).isEqualTo(httpRequest.getBodyAsString());
        }

        @Test
        void test_build_PUT_request_with_body_as_string() throws IOException {

            //given
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());
            HttpRequest httpRequest = new HttpRequest("http://dummy.com/", HttpRequest.Method.PUT);
            httpRequest.setBodyAsString("stuff");

            //given
            Request request = client.buildRequest(httpRequest);

            //then
            LOGGER.debug("request:{}", request);
            assertThat(request.url().url().toString()).hasToString(httpRequest.getUrl());
            assertThat(request.method()).isEqualTo(httpRequest.getMethod().name());
            RequestBody body = request.body();
            final Buffer buffer = new Buffer();
            body.writeTo(buffer);
            assertThat(buffer.readUtf8()).isEqualTo(httpRequest.getBodyAsString());
        }

        @Test
        void test_build_PUT_request_with_body_as_byte_array() throws IOException {

            //given
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());
            HttpRequest httpRequest = new HttpRequest("http://dummy.com/", HttpRequest.Method.PUT);
            httpRequest.setBodyAsByteArray("stuff".getBytes(StandardCharsets.UTF_8));

            //given
            Request request = client.buildRequest(httpRequest);

            //then
            LOGGER.debug("request:{}", request);
            assertThat(request.url().url().toString()).hasToString(httpRequest.getUrl());
            assertThat(request.method()).isEqualTo(httpRequest.getMethod().name());
            RequestBody body = request.body();
            final Buffer buffer = new Buffer();
            body.writeTo(buffer);
            String actual = buffer.readUtf8();
            byte[] decoded = Base64.getDecoder().decode(actual);
            assertThat(decoded).isEqualTo(httpRequest.getBodyAsByteArray());
        }

        @Test
        void test_build_POST_request_with_body_as_form() throws IOException {

            //given
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());
            HttpRequest httpRequest = new HttpRequest("http://dummy.com/", HttpRequest.Method.POST);
            Map<String, String> form = Maps.newHashMap();
            form.put("key1", "value1");
            form.put("key2", "value2");
            form.put("key3", "value3");
            httpRequest.setBodyAsForm(form);

            //given
            Request request = client.buildRequest(httpRequest);

            //then
            LOGGER.debug("request:{}", request);
            assertThat(request.url().url().toString()).hasToString(httpRequest.getUrl());
            assertThat(request.method()).isEqualTo(httpRequest.getMethod().name());
            RequestBody body = request.body();
            final Buffer buffer = new Buffer();
            body.writeTo(buffer);
            String actual = buffer.readUtf8();
            Map<String, String> keyValues = Splitter.on("&").withKeyValueSeparator("=").split(actual);
            assertThat(keyValues).isEqualTo(httpRequest.getBodyAsForm());
        }

        @Test
        void test_build_POST_request_with_body_as_multipart() throws IOException {

            //given
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());
            List<HttpPart> parts = Lists.newArrayList();
            String content1 = "content1";
            HttpPart httpPart1 = new HttpPart(Map.of("Content-Type", Lists.newArrayList("application/toto")), content1);
            parts.add(httpPart1);
            String content2 = "content2";
            HttpPart httpPart2 = new HttpPart(content2);
            parts.add(httpPart2);
            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("Content-Type", Lists.newArrayList("multipart/form-data; boundary=+++"));
            HttpRequest httpRequest = new HttpRequest("http://dummy.com/", HttpRequest.Method.POST, headers, HttpRequest.BodyType.MULTIPART, parts);

            //given
            Request request = client.buildRequest(httpRequest);

            //then
            LOGGER.debug("request:{}", request);
            assertThat(request.url().url().toString()).hasToString(httpRequest.getUrl());
            assertThat(request.method()).isEqualTo(httpRequest.getMethod().name());
            RequestBody body = request.body();
            final Buffer buffer = new Buffer();
            body.writeTo(buffer);
            String actual = buffer.readUtf8();
            String boundary = httpRequest.getBoundary();
            List<String> myParts = getMultiPartsAsString(actual, boundary);
            String part1AsString = myParts.get(0);
            Map<String, String> headers1 = getHeaders(part1AsString);
            assertThat(headers1.get("Content-Type")).contains("application/toto; charset=utf-8");
            assertThat(getPartContent(part1AsString)).isEqualTo(content1);
            String part2AsString = myParts.get(1);
            Map<String, String> headers2 = getHeaders(part2AsString);
            assertThat(headers2.get("Content-Type")).contains("application/json; charset=utf-8");
            assertThat(getPartContent(part2AsString)).isEqualTo(content2);
        }

        @Test
        void test_build_POST_request_with_body_as_multipart_with_file_upload() throws IOException, URISyntaxException {

            //given
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());
            List<HttpPart> parts = Lists.newArrayList();

            String content1 = "content1";
            HttpPart httpPart1 = new HttpPart(Map.of("Content-Type", Lists.newArrayList("application/toto")), content1);
            parts.add(httpPart1);

            String content2 = "content2";
            File nullFile = null;
            HttpPart httpPart2 = new HttpPart("parameter2", content2, nullFile);
            parts.add(httpPart2);

            URL fileUrl = Thread.currentThread().getContextClassLoader().getResource("upload.txt");
            File file = new File(fileUrl.toURI());
            HttpPart httpPart3 = new HttpPart("parameter3", "value3", file);
            parts.add(httpPart3);

            Map<String, List<String>> headers = Maps.newHashMap();
            headers.put("Content-Type", Lists.newArrayList("multipart/form-data; boundary=+++"));
            HttpRequest httpRequest = new HttpRequest("http://dummy.com/", HttpRequest.Method.POST, headers, HttpRequest.BodyType.MULTIPART, parts);

            //given
            Request request = client.buildRequest(httpRequest);

            //then
            LOGGER.debug("request:{}", request);
            assertThat(request.url().url().toString()).hasToString(httpRequest.getUrl());
            assertThat(request.method()).isEqualTo(httpRequest.getMethod().name());
            RequestBody body = request.body();
            final Buffer buffer = new Buffer();
            body.writeTo(buffer);
            String actual = buffer.readUtf8();
            String boundary = httpRequest.getBoundary();
            List<String> myParts = getMultiPartsAsString(actual, boundary);

            String part1AsString = myParts.get(0);
            Map<String, String> headers1 = getHeaders(part1AsString);
            assertThat(headers1.get("Content-Type")).contains("application/toto; charset=utf-8");
            assertThat(getPartContent(part1AsString)).isEqualTo(content1);

            String part2AsString = myParts.get(1);
            Map<String, String> headers2 = getHeaders(part2AsString);
            assertThat(headers2.get("Content-Disposition")).contains("form-data; name=\"parameter2\"");
            String part2Content = getPartContent(part2AsString);
            assertThat(part2Content).isEqualTo(content2);

            String part3AsString = myParts.get(2);
            Map<String, String> headers3 = getHeaders(part3AsString);
            String contentDisposition3 = headers3.get("Content-Disposition");
            assertThat(contentDisposition3).contains("form-data; name=\"parameter3\"; filename=\"upload.txt\"");
            String part3Content = getPartContent(part3AsString);
            assertThat(part3Content).isEqualTo("my content to upload\n" +
                    "test1\n" +
                    "test2");
        }

        @NotNull
        private List<String> getMultiPartsAsString(String actual, String boundary) {
            List<String> myParts = Lists.newArrayList(actual.split(Pattern.quote("--" + boundary)))
                    .stream()
                    .filter(s -> !s.isEmpty())
                    .filter(s -> !s.equals("--\r\n"))
                    .collect(Collectors.toList());
            return myParts;
        }

        private String getPartContent(String headersAndContentAsString) {
            return Lists.newArrayList(splitHeadersAndPartContent(headersAndContentAsString).get(1).split("\r\n")).get(0);
        }

        private Map<String, String> getHeaders(String headersAndContentAsString) {
            ArrayList<String> splits = splitHeadersAndPartContent(headersAndContentAsString);
            Map<String, String> headers = Maps.newHashMap();
            if (!splits.isEmpty()) {
                String headersContent = splits.get(0);
                headers.putAll(Lists.newArrayList(headersContent.split("\r\n")).stream()
                        .filter(s -> !s.isEmpty())
                        .map(s -> Lists.newArrayList(s.split(": ")))
                        .collect(Collectors.toMap(s -> s.get(0), s -> s.get(1))));
            }
            return headers;
        }

        @Test
        void test_build_GET_request_with_body_as_string() {

            //given
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());
            HttpRequest httpRequest = new HttpRequest("http://dummy.com/", HttpRequest.Method.GET);
            httpRequest.setBodyAsString("stuff");

            //when
            Request request = client.buildRequest(httpRequest);

            //then
            LOGGER.debug("request:{}", request);
            assertThat(request.url().url().toString()).hasToString(httpRequest.getUrl());
            assertThat(request.method()).isEqualTo(httpRequest.getMethod().name());
            assertThat(request.body()).isNull();
        }

    }

    @NotNull
    private static ArrayList<String> splitHeadersAndPartContent(String multipartContent) {
        return Lists.newArrayList(multipartContent.split("\r\n\r\n"));
    }

    @Nested
    class BuildResponse {
        @Test
        void test_build_response() {

            //given
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());

            HttpRequest httpRequest = new HttpRequest("http://dummy.com/", HttpRequest.Method.POST);
            httpRequest.setBodyAsString("stuff");
            Request request = client.buildRequest(httpRequest);

            Response.Builder builder = new Response.Builder();
            Headers headers = new Headers.Builder()
                    .add("key1", "value1")
                    .add(CONTENT_TYPE, APPLICATION_JSON)
                    .build();
            builder.headers(headers);
            builder.request(request);
            builder.code(200);
            builder.message("OK");
            String responseContent = "blabla";
            Buffer buffer = new Buffer();
            buffer.write(responseContent.getBytes(StandardCharsets.UTF_8));
            ResponseBody responseBody = new RealResponseBody(APPLICATION_JSON, responseContent.length(), buffer);
            builder.body(responseBody);
            builder.protocol(Protocol.HTTP_1_1);
            Response response = builder.build();
            HttpResponseBuilder httpResponseBuilder = new HttpResponseBuilder(1024, 8000,100_000);
            //when
            HttpResponse httpResponse = client.buildResponse(httpResponseBuilder, response);

            //then
            LOGGER.debug("response:{}", response);
            assertThat(response.code()).isEqualTo(httpResponse.getStatusCode());
            assertThat(response.message()).isEqualTo(httpResponse.getStatusMessage());
            assertThat(response.header("key1")).isEqualTo(httpResponse.getHeaders().get("key1").get(0));
            assertThat(response.header(CONTENT_TYPE)).isEqualTo(httpResponse.getHeaders().get(CONTENT_TYPE).get(0));

        }

        @Test
        void test_build_response_with_status_message_limit() {

            //given
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(CONFIG_DEFAULT_HTTP_RESPONSE_MESSAGE_STATUS_LIMIT, 4);
            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());

            HttpRequest httpRequest = new HttpRequest("http://dummy.com/", HttpRequest.Method.POST);
            httpRequest.setBodyAsString("stuff");
            Request request = client.buildRequest(httpRequest);

            Response.Builder builder = new Response.Builder();
            Headers headers = new Headers.Builder()
                    .add("key1", "value1")
                    .add(CONTENT_TYPE, APPLICATION_JSON)
                    .build();
            builder.headers(headers);
            builder.request(request);
            builder.code(200);
            builder.message("OK!!!!!!!");
            String responseContent = "blabla";
            Buffer buffer = new Buffer();
            buffer.write(responseContent.getBytes(StandardCharsets.UTF_8));
            ResponseBody responseBody = new RealResponseBody(APPLICATION_JSON, responseContent.length(), buffer);
            builder.body(responseBody);
            builder.protocol(Protocol.HTTP_1_1);
            Response response = builder.build();
            HttpResponseBuilder httpResponseBuilder = new HttpResponseBuilder(4, 8000,100_000);
            //when
            HttpResponse httpResponse = client.buildResponse(httpResponseBuilder, response);

            //then
            LOGGER.debug("response:{}", response);
            assertThat(response.code()).isEqualTo(httpResponse.getStatusCode());
            assertThat(httpResponse.getStatusMessage()).isEqualTo("OK!!");
            assertThat(response.header("key1")).isEqualTo(httpResponse.getHeaders().get("key1").get(0));
            assertThat(response.header(CONTENT_TYPE)).isEqualTo(httpResponse.getHeaders().get(CONTENT_TYPE).get(0));

        }

        @Test
        void test_build_response_with_headers_limit() {

            //given
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(CONFIG_DEFAULT_HTTP_RESPONSE_HEADERS_LIMIT_DOC, 20);
            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());

            HttpRequest httpRequest = new HttpRequest("http://dummy.com/", HttpRequest.Method.POST);
            httpRequest.setBodyAsString("stuff");
            Request request = client.buildRequest(httpRequest);

            Response.Builder builder = new Response.Builder();
            Headers headers = new Headers.Builder()
                    .add("key1", "value1")
                    .add(CONTENT_TYPE, APPLICATION_JSON)
                    .build();
            builder.headers(headers);
            builder.request(request);
            builder.code(200);
            builder.message("OK!!!!!!!");
            String responseContent = "blabla78965555";
            Buffer buffer = new Buffer();
            buffer.write(responseContent.getBytes(StandardCharsets.UTF_8));
            ResponseBody responseBody = new RealResponseBody(APPLICATION_JSON, responseContent.length(), buffer);
            builder.body(responseBody);
            builder.protocol(Protocol.HTTP_1_1);
            Response response = builder.build();
            HttpResponseBuilder httpResponseBuilder = new HttpResponseBuilder(Integer.MAX_VALUE, 8000,10);
            //when
            HttpResponse httpResponse = client.buildResponse(httpResponseBuilder, response);

            //then
            LOGGER.debug("response:{}", response);
            assertThat(response.code()).isEqualTo(httpResponse.getStatusCode());
            assertThat(httpResponse.getStatusMessage()).isEqualTo("OK!!!!!!!");
            assertThat(httpResponse.getBodyAsString()).isEqualTo("blabla7896");
            assertThat(response.header("key1")).isEqualTo(httpResponse.getHeaders().get("key1").get(0));
            assertThat(response.header(CONTENT_TYPE)).isEqualTo(httpResponse.getHeaders().get(CONTENT_TYPE).get(0));

        }



        @Test
        void test_build_response_with_body_limit() {

            //given
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(CONFIG_DEFAULT_HTTP_RESPONSE_BODY_LIMIT, 10);
            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());

            HttpRequest httpRequest = new HttpRequest("http://dummy.com/", HttpRequest.Method.POST);
            httpRequest.setBodyAsString("stuff");
            Request request = client.buildRequest(httpRequest);

            Response.Builder builder = new Response.Builder();
            Headers headers = new Headers.Builder()
                    .add("key1", "value1")
                    .add(CONTENT_TYPE, APPLICATION_JSON)
                    .build();
            builder.headers(headers);
            builder.request(request);
            builder.code(200);
            builder.message("OK!!!!!!!");
            String responseContent = "blabla78965555";
            Buffer buffer = new Buffer();
            buffer.write(responseContent.getBytes(StandardCharsets.UTF_8));
            ResponseBody responseBody = new RealResponseBody(APPLICATION_JSON, responseContent.length(), buffer);
            builder.body(responseBody);
            builder.protocol(Protocol.HTTP_1_1);
            Response response = builder.build();
            HttpResponseBuilder httpResponseBuilder = new HttpResponseBuilder(Integer.MAX_VALUE, 8000,10);
            //when
            HttpResponse httpResponse = client.buildResponse(httpResponseBuilder, response);

            //then
            LOGGER.debug("response:{}", response);
            assertThat(response.code()).isEqualTo(httpResponse.getStatusCode());
            assertThat(httpResponse.getStatusMessage()).isEqualTo("OK!!!!!!!");
            assertThat(httpResponse.getBodyAsString()).isEqualTo("blabla7896");
            assertThat(response.header("key1")).isEqualTo(httpResponse.getHeaders().get("key1").get(0));
            assertThat(response.header(CONTENT_TYPE)).isEqualTo(httpResponse.getHeaders().get(CONTENT_TYPE).get(0));

        }


    }

    @Nested
    class Cache {
        @Test
        void test_activated_cache_with_file_type() {
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(OKHTTP_CACHE_ACTIVATE, "true");
            org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry()));
        }

        @Test
        void test_activated_cache_with_file_type_and_max_entries() {
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(OKHTTP_CACHE_ACTIVATE, "true");
            config.put(OKHTTP_CACHE_MAX_SIZE, "50000");
            org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry()));

        }

        @Test
        void test_activated_cache_with_file_type_and_max_entries_and_location() {
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(OKHTTP_CACHE_ACTIVATE, "true");
            config.put(OKHTTP_CACHE_MAX_SIZE, "50000");
            config.put(OKHTTP_CACHE_DIRECTORY_PATH, "/tmp/toto");
            org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry()));
        }

        @Test
        void test_activated_cache_with_inmemory_type() {
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(OKHTTP_CACHE_ACTIVATE, "true");
            config.put(OKHTTP_CACHE_TYPE, "inmemory");
            org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry()));
        }

        @Test
        void test_inactivated_cache() {
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(OKHTTP_CACHE_ACTIVATE, "false");
            org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry()));
        }

        @Test
        void test_no_cache() {
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry()));
        }
    }

    @Nested
    class TestRateLimiter {
        @Test
        @DisplayName("test rate limiter nominal case")
        void test_nominal_case() throws ExecutionException, InterruptedException {


            //given
            //scenario
            String scenario = "activating logging interceptor";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            String bodyResponse = "{\"result\":\"pong\"}";
            wireMock.register(WireMock.post("/ping").inScenario(scenario)
                    .whenScenarioStateIs(STARTED)
                    .willReturn(WireMock.aResponse()
                            .withBody(bodyResponse)
                            .withStatus(200)
                            .withStatusMessage("OK")
                    )
            );


            //build http client
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put("rate.limiter.max.executions", "1");

            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new OkHttpClient(
                    config,
                    null,
                    new Random(),
                    null,
                    null,
                    getCompositeMeterRegistry()
            );

            HttpRequest httpRequest = getHttpRequest(wmRuntimeInfo);
            Stopwatch stopwatch = Stopwatch.createStarted();
            List<HttpExchange> exchanges = Lists.newArrayList();
            //call web service
            for (int i = 0; i < 10; i++) {
                HttpExchange httpExchange1 = client.call(httpRequest, new AtomicInteger(1)).get();
                assertThat(httpExchange1.getHttpResponse().getStatusCode()).isEqualTo(200);
                exchanges.add(httpExchange1);
            }
            stopwatch.stop();
            long elapsedMillis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            assertThat(elapsedMillis).isGreaterThan(7895);
            for (HttpExchange exchange : exchanges) {
                LOGGER.info("httpExchange direct time '{}' ms", exchange.getDurationInMillis());
            }
        }

        @Test
        @DisplayName("test without rate limiter")
        void test_without_rate_limiter() throws ExecutionException, InterruptedException {


            //given
            //scenario
            String scenario = "activating logging interceptor";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            String bodyResponse = "{\"result\":\"pong\"}";
            wireMock.register(WireMock.post("/ping").inScenario(scenario)
                    .whenScenarioStateIs(STARTED)
                    .willReturn(WireMock.aResponse()
                            .withBody(bodyResponse)
                            .withStatus(200)
                            .withStatusMessage("OK")
                    )
            );


            //build http client
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put("dummy.config", "1");

            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new OkHttpClient(
                    config,
                    null,
                    new Random(),
                    null,
                    null,
                    getCompositeMeterRegistry()
            );

            HttpRequest httpRequest = getHttpRequest(wmRuntimeInfo);
            Stopwatch stopwatch = Stopwatch.createStarted();
            List<HttpExchange> exchanges = Lists.newArrayList();
            //call web service
            for (int i = 0; i < 10; i++) {
                HttpExchange httpExchange1 = client.call(httpRequest, new AtomicInteger(1)).get();
                assertThat(httpExchange1.getHttpResponse().getStatusCode()).isEqualTo(200);
                exchanges.add(httpExchange1);
            }
            stopwatch.stop();
            long elapsedMillis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            assertThat(elapsedMillis).isLessThan(3000);
            for (HttpExchange exchange : exchanges) {
                LOGGER.info("httpExchange direct time '{}' ms", exchange.getDurationInMillis());
            }
        }

        @Test
        @DisplayName("test with shared rate limiter")
        void test_with_shared_rate_limiter() throws ExecutionException, InterruptedException {


            //given
            //scenario
            String scenario = "activating logging interceptor";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            String bodyResponse = "{\"result\":\"pong\"}";
            wireMock.register(WireMock.post("/ping").inScenario(scenario)
                    .whenScenarioStateIs(STARTED)
                    .willReturn(WireMock.aResponse()
                            .withBody(bodyResponse)
                            .withStatus(200)
                            .withStatusMessage("OK")
                    )
            );


            //build http client
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put("rate.limiter.max.executions", "1");
            config.put("rate.limiter.scope", "static");

            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client1 = new OkHttpClient(
                    config,
                    null,
                    new Random(),
                    null,
                    null,
                    getCompositeMeterRegistry()
            );
            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client2 = new OkHttpClient(
                    config,
                    null,
                    new Random(),
                    null,
                    null,
                    getCompositeMeterRegistry()
            );

            HttpRequest httpRequest = getHttpRequest(wmRuntimeInfo);
            Stopwatch stopwatch = Stopwatch.createStarted();
            List<HttpExchange> exchanges = Lists.newArrayList();
            //call web service
            for (int i = 0; i < 5; i++) {
                HttpExchange httpExchange1 = client1.call(httpRequest, new AtomicInteger(1)).get();
                assertThat(httpExchange1.getHttpResponse().getStatusCode()).isEqualTo(200);
                exchanges.add(httpExchange1);
                HttpExchange httpExchange2 = client2.call(httpRequest, new AtomicInteger(1)).get();
                assertThat(httpExchange2.getHttpResponse().getStatusCode()).isEqualTo(200);
                exchanges.add(httpExchange2);
            }
            stopwatch.stop();
            long elapsedMillis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            assertThat(elapsedMillis).isGreaterThan(7895);
            for (HttpExchange exchange : exchanges) {
                LOGGER.info("httpExchange direct time '{}' ms", exchange.getDurationInMillis());
            }
        }

        @Test
        @DisplayName("test with multiple clients without shared rate limiter")
        void test_with_multiple_client_without_shared_rate_limiter() throws ExecutionException, InterruptedException {


            //given
            //scenario
            String scenario = "activating logging interceptor";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            String bodyResponse = "{\"result\":\"pong\"}";
            wireMock.register(WireMock.post("/ping").inScenario(scenario)
                    .whenScenarioStateIs(STARTED)
                    .willReturn(WireMock.aResponse()
                            .withBody(bodyResponse)
                            .withStatus(200)
                            .withStatusMessage("OK")
                    )
            );


            //build http client
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put("rate.limiter.max.executions", "1");

            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client1 = new OkHttpClient(
                    config,
                    null,
                    new Random(),
                    null,
                    null,
                    getCompositeMeterRegistry()
            );
            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client2 = new OkHttpClient(
                    config,
                    null,
                    new Random(),
                    null,
                    null,
                    getCompositeMeterRegistry()
            );

            HttpRequest httpRequest = getHttpRequest(wmRuntimeInfo);
            Stopwatch stopwatch = Stopwatch.createStarted();
            List<HttpExchange> exchanges = Lists.newArrayList();
            //call web service
            for (int i = 0; i < 5; i++) {
                HttpExchange httpExchange1 = client1.call(httpRequest, new AtomicInteger(1)).get();
                assertThat(httpExchange1.getHttpResponse().getStatusCode()).isEqualTo(200);
                exchanges.add(httpExchange1);
                HttpExchange httpExchange2 = client2.call(httpRequest, new AtomicInteger(1)).get();
                assertThat(httpExchange2.getHttpResponse().getStatusCode()).isEqualTo(200);
                exchanges.add(httpExchange2);
            }
            stopwatch.stop();
            long elapsedMillis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            assertThat(elapsedMillis).isLessThan(7895);
            for (HttpExchange exchange : exchanges) {
                LOGGER.info("httpExchange direct time '{}' ms", exchange.getDurationInMillis());
            }
        }

        @NotNull
        private HttpRequest getHttpRequest(WireMockRuntimeInfo wmRuntimeInfo) {
            String baseUrl = "http://" + getIP() + ":" + wmRuntimeInfo.getHttpPort();
            String url = baseUrl + "/ping";
            HashMap<String, List<String>> headers = Maps.newHashMap();
            headers.put(CONTENT_TYPE, Lists.newArrayList("text/plain"));
            headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
            headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
            headers.put("User-Agent", Lists.newArrayList("toto"));
            HttpRequest httpRequest = new HttpRequest(
                    url,
                    HttpRequest.Method.POST
            );
            httpRequest.setHeaders(headers);
            httpRequest.setBodyAsString("stuff");
            return httpRequest;
        }

    }

    @Nested
    class TestNativeCall {
        @Test
        @DisplayName("test nominal case")
        void test_nominal_case() throws ExecutionException, InterruptedException {
            String bodyResponse = "{\"result\":\"pong\"}";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();

            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());

            String baseUrl = "http://" + getIP() + ":" + wmRuntimeInfo.getHttpPort();
            String url = baseUrl + "/ping";
            HashMap<String, List<String>> headers = Maps.newHashMap();
            headers.put(CONTENT_TYPE, Lists.newArrayList("text/plain"));
            headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
            headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
            headers.put("User-Agent", Lists.newArrayList("toto"));
            HttpRequest httpRequest = new HttpRequest(
                    url,
                    HttpRequest.Method.POST
            );
            httpRequest.setHeaders(headers);
            httpRequest.setBodyAsString("stuff");


            String scenario = "activating logging interceptor";
            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs(STARTED)
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(ACCESS_GRANTED_STATE)
                    );

            HttpExchange httpExchange1 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange1.getHttpResponse().getStatusCode()).isEqualTo(200);
        }

        @Test
        @DisplayName("test activate logging interceptor")
        void test_activating_logging_interceptor() throws ExecutionException, InterruptedException {
            String bodyResponse = "{\"result\":\"pong\"}";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();

            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_LOGGING_ACTIVATE, "true");
            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());

            String baseUrl = "http://" + getIP() + ":" + wmRuntimeInfo.getHttpPort();
            String url = baseUrl + "/ping";
            HashMap<String, List<String>> headers = Maps.newHashMap();
            headers.put(CONTENT_TYPE, Lists.newArrayList("text/plain"));
            headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
            headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
            HttpRequest httpRequest = new HttpRequest(
                    url,
                    HttpRequest.Method.POST
            );
            httpRequest.setHeaders(headers);
            httpRequest.setBodyAsString("stuff");


            String scenario = "activating logging interceptor";
            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs(STARTED)
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(ACCESS_GRANTED_STATE)
                    );

            HttpExchange httpExchange1 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange1.getHttpResponse().getStatusCode()).isEqualTo(200);


        }


    }

    @Nested
    class TestAuthentication {
        @Test
        @DisplayName("test Basic Authentication : two calls")
        void test_basic_authentication() throws ExecutionException, InterruptedException {

            String username = "user1";
            String password = "password1";
            String bodyResponse = "{\"result\":\"pong\"}";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();

            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put("httpclient.authentication.basic.activate", true);
            config.put("httpclient.authentication.basic.username", username);
            config.put("httpclient.authentication.basic.password", password);

            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());

            String baseUrl = "http://" + getIP() + ":" + wmRuntimeInfo.getHttpPort();
            String url = baseUrl + "/ping";
            HashMap<String, List<String>> headers = Maps.newHashMap();
            headers.put(CONTENT_TYPE, Lists.newArrayList("text/plain"));
            headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
            headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
            HttpRequest httpRequest = new HttpRequest(
                    url,
                    HttpRequest.Method.POST
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
                            ).willSetStateTo(UNAUTHORIZED_STATE)
                    );
            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs(UNAUTHORIZED_STATE)
                            .withBasicAuth(username, password)
                            .withHeader(CONTENT_TYPE, containing("text/plain"))
                            .withHeader("X-Correlation-ID", containing("e6de70d1-f222-46e8-b755-754880687822"))
                            .withHeader("X-Request-ID", containing("e6de70d1-f222-46e8-b755-11111"))
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(ACCESS_GRANTED_STATE)
                    );
            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs(ACCESS_GRANTED_STATE)
                            .withBasicAuth(username, password)
                            .withHeader(CONTENT_TYPE, containing("text/plain"))
                            .withHeader("X-Correlation-ID", containing("e6de70d1-f222-46e8-b755-754880687822"))
                            .withHeader("X-Request-ID", containing("e6de70d1-f222-46e8-b755-11111"))
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(ACCESS_GRANTED_STATE)
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
            config.put(CONFIGURATION_ID, "default");
            config.put("httpclient.authentication.digest.activate", true);
            config.put("httpclient.authentication.digest.username", username);
            config.put("httpclient.authentication.digest.password", password);
            Random random = getFixedRandom();
            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, random, null, null, getCompositeMeterRegistry());

            String baseUrl = "http://" + getIP() + ":" + wmRuntimeInfo.getHttpPort();
            String url = baseUrl + "/ping";
            HashMap<String, List<String>> headers = Maps.newHashMap();
            headers.put(CONTENT_TYPE, Lists.newArrayList("text/plain"));
            headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
            headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
            HttpRequest httpRequest = new HttpRequest(
                    url,
                    HttpRequest.Method.POST
            );
            httpRequest.setHeaders(headers);
            httpRequest.setBodyAsString("stuff");

            String url2 = baseUrl + "/ping2";
            HashMap<String, List<String>> headers2 = Maps.newHashMap();
            headers2.put(CONTENT_TYPE, Lists.newArrayList("text/plain"));
            headers2.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
            headers2.put("X-Request-ID", Lists.newArrayList("22222-33333-000-000-0000"));
            HttpRequest httpRequest2 = new HttpRequest(
                    url2,
                    HttpRequest.Method.POST
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
                            ).willSetStateTo(UNAUTHORIZED_STATE)
                    );

            wireMock
                    .register(WireMock.post("/ping")
                            .withName("2")
                            .inScenario(scenario)
                            .whenScenarioStateIs(UNAUTHORIZED_STATE)
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
                            .withHeader(CONTENT_TYPE, containing("text/plain"))
                            .withHeader("X-Correlation-ID", equalTo("e6de70d1-f222-46e8-b755-754880687822"))
                            .withHeader("X-Request-ID", equalTo("e6de70d1-f222-46e8-b755-11111"))
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(ACCESS_GRANTED_STATE)
                    );

            wireMock
                    .register(WireMock.post("/ping2")
                            .withName("3")
                            .inScenario(scenario)
                            .whenScenarioStateIs(ACCESS_GRANTED_STATE)
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
                            .withHeader(CONTENT_TYPE, containing("text/plain"))
                            .withHeader("X-Correlation-ID", equalTo("e6de70d1-f222-46e8-b755-754880687822"))
                            .withHeader("X-Request-ID", equalTo("22222-33333-000-000-0000"))
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(ACCESS_GRANTED_STATE)
                    );


            HttpExchange httpExchange1 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange1.getHttpResponse().getStatusCode()).isEqualTo(200);

            HttpExchange httpExchange2 = client.call(httpRequest2, new AtomicInteger(1)).get();
            assertThat(httpExchange2.getHttpResponse().getStatusCode()).isEqualTo(200);

        }

        @Test
        void test_oauth2_client_credentials_flow_authentication_with_client_secret_basic() throws ExecutionException, InterruptedException, IOException {
            String scenario = "Oauth2 Client Credentials flow Authentication with client_secret_basic";
            String wellKnownOpenidConfiguration = "/.well-known/openid-configuration";
            String wellKnownOk = "WellKnownOk";
            String tokenOk = "TokenOk";
            String clientId = "44d34a4d05344c97837d463207805f8b";
            String clientSecret = "3fc0576720544ac293a3a5304e6c0fa8";
            String bodyResponse = "{\"result\":\"pong\"}";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            String httpBaseUrl = wmRuntimeInfo.getHttpBaseUrl();
            String wellKnownUrl = httpBaseUrl + "/.well-known/openid-configuration";

            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_ACTIVATE, true);
            config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_WELL_KNOWN_URL, wellKnownUrl);
            config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_ID, clientId);
            config.put(HTTP_CLIENT_AUTHENTICATION_OAUTH2_CLIENT_CREDENTIALS_FLOW_CLIENT_SECRET, clientSecret);


            String url = httpBaseUrl + "/v1/tracks/2TpxZ7JUBn3uw46aR7qd6V";
            HashMap<String, List<String>> headers = Maps.newHashMap();
            headers.put(CONTENT_TYPE, Lists.newArrayList("text/plain"));
            headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
            headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
            HttpRequest httpRequest = new HttpRequest(
                    url,
                    HttpRequest.Method.GET
            );
            httpRequest.setHeaders(headers);

            Path path = Paths.get("src/test/resources/oauth2/wellknownUrlContent.json");
            httpBaseUrl = wmRuntimeInfo.getHttpBaseUrl();
            String content = Files.readString(path);
            String wellKnownUrlContent = content.replaceAll("baseUrl", httpBaseUrl);

            //good well known content
            wireMock
                    .register(WireMock.get(wellKnownOpenidConfiguration).inScenario(scenario)
                            .whenScenarioStateIs(STARTED)
                            .willReturn(WireMock.aResponse()
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                                    .withBody(wellKnownUrlContent)
                            ).willSetStateTo(wellKnownOk)
                    );

            Path tokenPath = Paths.get("src/test/resources/oauth2/token.json");
            String tokenContent = Files.readString(tokenPath);
            wireMock
                    .register(
                            WireMock.post("/api/token")
                                    .withHeader("Content-Type", containing("application/x-www-form-urlencoded; charset=UTF-8"))
                                    .withHeader("Authorization", containing("Basic NDRkMzRhNGQwNTM0NGM5NzgzN2Q0NjMyMDc4MDVmOGI6M2ZjMDU3NjcyMDU0NGFjMjkzYTNhNTMwNGU2YzBmYTg="))
                                    .inScenario(scenario)
                                    .whenScenarioStateIs(UNAUTHORIZED_STATE)
                                    .willReturn(WireMock.aResponse()
                                            .withStatus(200)
                                            .withStatusMessage("OK")
                                            .withBody(tokenContent)
                                    ).willSetStateTo(tokenOk)
                    );


            wireMock
                    .register(WireMock.get("/v1/tracks/2TpxZ7JUBn3uw46aR7qd6V")
                            .withName("1")
                            .inScenario(scenario)
                            .whenScenarioStateIs(wellKnownOk)
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Date", "Wed, 21 Oct 2022 05:21:23 GMT")
                                    .withHeader("WWW-Authenticate",
                                            "Bearer")
                                    .withStatus(401)
                                    .withStatusMessage("Unauthorized")
                            ).willSetStateTo(UNAUTHORIZED_STATE)
                    );

            wireMock
                    .register(WireMock.get("/v1/tracks/2TpxZ7JUBn3uw46aR7qd6V")
                            .withName("2")
                            .inScenario(scenario)
                            .whenScenarioStateIs(tokenOk)
                            .withHeader("Authorization",
                                    equalTo("Bearer BQDzs98uhifaGayk8H9tCTRozufhFmgV_HKMCnnDdMTdz1FcOo3sdj8OZJ_azo96LRdLI9_1uJOCXxbGZme11KCb6ZxTuCt8B5FxEeECb1kO_-UDuf8")
                            )
                            .withHeader("X-Correlation-ID", equalTo("e6de70d1-f222-46e8-b755-754880687822"))
                            .withHeader("X-Request-ID", equalTo("e6de70d1-f222-46e8-b755-11111"))
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(ACCESS_GRANTED_STATE)
                    );


            Random random = getFixedRandom();
            OkHttpClient client = new OkHttpClient(config, null, random, null, null, getCompositeMeterRegistry());

            HttpExchange httpExchange1 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange1.getHttpResponse().getStatusCode()).isEqualTo(200);

        }

        private Random getFixedRandom() {
            Random random = mock(Random.class);
            byte[] randomBytes = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07};

            doAnswer(invocation -> {
                Object[] args = invocation.getArguments();
                byte[] rnd = (byte[]) args[0];
                System.arraycopy(randomBytes, 0, rnd, 0, randomBytes.length);
                return null;
            }).when(random).nextBytes(any(byte[].class));
            return random;
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
            config.put(CONFIGURATION_ID, "default");
            Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(getIP(), wmRuntimeInfo.getHttpPort()));


            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), proxy, null, getCompositeMeterRegistry());

            HashMap<String, List<String>> headers = Maps.newHashMap();
            headers.put(CONTENT_TYPE, Lists.newArrayList("text/plain"));
            headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
            headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
            HttpRequest httpRequest = new HttpRequest(
                    url,
                    HttpRequest.Method.POST
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
            config.put(CONFIGURATION_ID, "default");
            config.put(HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_ACTIVATE, true);
            String username = "user1";
            config.put(HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_USERNAME, username);
            String password = "password1";
            config.put(HTTP_CLIENT_PROXY_AUTHENTICATION_BASIC_PASSWORD, password);
            Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(getIP(), wmRuntimeInfo.getHttpPort()));

            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), proxy, null, getCompositeMeterRegistry());

            HashMap<String, List<String>> headers = Maps.newHashMap();
            headers.put(CONTENT_TYPE, Lists.newArrayList("text/plain"));
            headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
            headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
            HttpRequest httpRequest = new HttpRequest(
                    url,
                    HttpRequest.Method.POST
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
                            .withHeader(CONTENT_TYPE, containing("text/plain"))
                            .withHeader("X-Correlation-ID", containing("e6de70d1-f222-46e8-b755-754880687822"))
                            .withHeader("X-Request-ID", containing("e6de70d1-f222-46e8-b755-11111"))
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(ACCESS_GRANTED_STATE)
                    );
            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs(ACCESS_GRANTED_STATE)
                            .withHeader(CONTENT_TYPE, containing("text/plain"))
                            .withHeader("X-Correlation-ID", containing("e6de70d1-f222-46e8-b755-754880687822"))
                            .withHeader("X-Request-ID", containing("e6de70d1-f222-46e8-b755-11111"))
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(ACCESS_GRANTED_STATE)
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
            config.put(CONFIGURATION_ID, "default");
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
            Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(getIP(), wmRuntimeInfo.getHttpPort()));

            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), proxy, null, getCompositeMeterRegistry());

            HashMap<String, List<String>> headers = Maps.newHashMap();
            headers.put(CONTENT_TYPE, Lists.newArrayList("text/plain"));
            headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
            headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
            HttpRequest httpRequest = new HttpRequest(
                    url,
                    HttpRequest.Method.POST
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
                            .withHeader(CONTENT_TYPE, containing("text/plain"))
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
                            .withHeader(CONTENT_TYPE, containing("text/plain"))
                            .withHeader("X-Correlation-ID", containing("e6de70d1-f222-46e8-b755-754880687822"))
                            .withHeader("X-Request-ID", containing("e6de70d1-f222-46e8-b755-11111"))
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(ACCESS_GRANTED_STATE)
                    );
            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .whenScenarioStateIs(ACCESS_GRANTED_STATE)
                            .withHeader("Proxy-Authorization", containing("Basic cHJveHl1c2VyMTpwcm94eXBhc3N3b3JkMQ=="))
                            .withBasicAuth(username, password)
                            .withHeader(CONTENT_TYPE, containing("text/plain"))
                            .withHeader("X-Correlation-ID", containing("e6de70d1-f222-46e8-b755-754880687822"))
                            .withHeader("X-Request-ID", containing("e6de70d1-f222-46e8-b755-11111"))
                            .willReturn(WireMock.aResponse()
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(ACCESS_GRANTED_STATE)
                    );

            HttpExchange httpExchange1 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange1.getHttpResponse().getStatusCode()).isEqualTo(200);
            HttpExchange httpExchange2 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange2.getHttpResponse().getStatusCode()).isEqualTo(200);

        }

        @Test
        @DisplayName("test proxy with digest authentication and digest authentication on website")
        void test_proxy_with_digest_authentication_and_digest_authentication_on_website() throws ExecutionException, InterruptedException {

            String bodyResponse = "{\"result\":\"pong\"}";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            //the test will call the proxy to try to forward the request, but wiremock won't relay.
            String baseUrl = "http://" + getIP() + ":" + wmHttp.getPort();
            String url = baseUrl + "/ping";

            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_ACTIVATE, true);
            String proxyUsername = "proxyuser1";
            config.put(HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_USERNAME, proxyUsername);
            String proxyPassword = "proxypassword1";
            config.put(HTTP_CLIENT_PROXY_AUTHENTICATION_DIGEST_PASSWORD, proxyPassword);

            config.put("httpclient.authentication.digest.activate", true);
            String username = "user1";
            String password = "password1";
            config.put("httpclient.authentication.digest.username", username);
            config.put("httpclient.authentication.digest.password", password);
            Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(getIP(), wmRuntimeInfo.getHttpPort()));
            Random random = mock(Random.class);
            byte[] randomBytes = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07};
            doAnswer(invocation -> {
                Object[] args = invocation.getArguments();
                byte[] rnd = (byte[]) args[0];
                System.arraycopy(randomBytes, 0, rnd, 0, randomBytes.length);
                return null;
            }).when(random).nextBytes(any(byte[].class));

            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, random, proxy, null, getCompositeMeterRegistry());

            HashMap<String, List<String>> headers = Maps.newHashMap();
            headers.put(CONTENT_TYPE, Lists.newArrayList("text/plain"));
            headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
            headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
            HttpRequest httpRequest = new HttpRequest(
                    url,
                    HttpRequest.Method.GET
            );
            httpRequest.setHeaders(headers);


            String scenario = "Proxy with authentication";

            wireMock
                    .register(WireMock.get("/ping").inScenario(scenario)
                            .whenScenarioStateIs(STARTED)
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Date", "Wed, 21 Oct 2022 05:21:23 GMT")
                                    .withHeader("Proxy-Authenticate",
                                            "Digest " +
                                                    "realm=\"Access to proxy site\"," +
                                                    "qop=\"auth,auth-int\"," +
                                                    "nonce=\"dcd98b7102dd2f0e8b11d0f615bfb0c093\"," +
                                                    "opaque=\"5cdc029c403ebaf9f0171e9517f40e41\"")
                                    .withStatus(407)
                                    .withStatusMessage("Proxy Authentication Required")
                            ).willSetStateTo(PROXY_AUTHENTICATION_REQUIRED_STATE)
                    );
            wireMock
                    .register(WireMock.get("/ping").inScenario(scenario)
                            .whenScenarioStateIs(PROXY_AUTHENTICATION_REQUIRED_STATE)
                            .withHeader("Proxy-Authorization",
                                    equalTo("Digest " +
                                            "username=\"proxyuser1\", " +
                                            "realm=\"Access to proxy site\", " +
                                            "nonce=\"dcd98b7102dd2f0e8b11d0f615bfb0c093\", " +
                                            "uri=\"/ping\", " +
                                            "response=\"e9920e89b8c768223a62dda432a33ab1\", " +
                                            "qop=auth, " +
                                            "nc=00000001, " +
                                            "cnonce=\"0001020304050607\", " +
                                            "algorithm=MD5, " +
                                            "opaque=\"5cdc029c403ebaf9f0171e9517f40e41\""
                                    )
                            )
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Date", "Wed, 21 Oct 2022 05:21:23 GMT")
                                    .withHeader("WWW-Authenticate", "Digest " +
                                            "realm=\"Access to web site\"," +
                                            "qop=\"auth,auth-int\"," +
                                            "nonce=\"aad55b7102dd2f0e8c99d123456fb0c011\"," +
                                            "opaque=\"5caa029c403ebaf9f3333e9517f40e66\"")
                                    .withStatus(401)
                                    .withStatusMessage("Unauthorized")
                            ).willSetStateTo(UNAUTHORIZED_STATE)
                    );
            wireMock
                    .register(WireMock.get("/ping").inScenario(scenario)
                            .whenScenarioStateIs(UNAUTHORIZED_STATE)
                            .withHeader("Proxy-Authorization",
                                    equalTo("Digest " +
                                            "username=\"proxyuser1\", " +
                                            "realm=\"Access to proxy site\", " +
                                            "nonce=\"dcd98b7102dd2f0e8b11d0f615bfb0c093\", " +
                                            "uri=\"/ping\", " +
                                            "response=\"e9920e89b8c768223a62dda432a33ab1\", " +
                                            "qop=auth, " +
                                            "nc=00000001, " +
                                            "cnonce=\"0001020304050607\", " +
                                            "algorithm=MD5, " +
                                            "opaque=\"5cdc029c403ebaf9f0171e9517f40e41\""
                                    )
                            )
                            .withHeader("Authorization",
                                    equalTo("Digest " +
                                            "username=\"user1\", " +
                                            "realm=\"Access to web site\", " +
                                            "nonce=\"aad55b7102dd2f0e8c99d123456fb0c011\", " +
                                            "uri=\"/ping\", " +
                                            "response=\"cbe92e92eb135ebea5c11fdf80d728d4\", " +
                                            "qop=auth, " +
                                            "nc=00000001, " +
                                            "cnonce=\"0001020304050607\", " +
                                            "algorithm=MD5, " +
                                            "opaque=\"5caa029c403ebaf9f3333e9517f40e66\""
                                    )
                            )
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content/Type", "text/plain")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(ACCESS_GRANTED_STATE)
                    );
            wireMock
                    .register(WireMock.get("/ping").inScenario(scenario)
                            .whenScenarioStateIs(ACCESS_GRANTED_STATE)
                            .withHeader("Proxy-Authorization",
                                    equalTo("Digest " +
                                            "username=\"proxyuser1\", " +
                                            "realm=\"Access to proxy site\", " +
                                            "nonce=\"dcd98b7102dd2f0e8b11d0f615bfb0c093\", " +
                                            "uri=\"/ping\", " +
                                            "response=\"ab6c6c6d8399935a747dba23f84d99e1\", " +
                                            "qop=auth, " +
                                            "nc=00000002, " +
                                            "cnonce=\"0001020304050607\", " +
                                            "algorithm=MD5, " +
                                            "opaque=\"5cdc029c403ebaf9f0171e9517f40e41\""
                                    )
                            )
                            .withHeader("Authorization",
                                    equalTo("Digest " +
                                            "username=\"user1\", " +
                                            "realm=\"Access to web site\", " +
                                            "nonce=\"aad55b7102dd2f0e8c99d123456fb0c011\", " +
                                            "uri=\"/ping\", " +
                                            "response=\"3855fd23f16597806e6df635bf4a40fb\", " +
                                            "qop=auth, " +
                                            "nc=00000002, " +
                                            "cnonce=\"0001020304050607\", " +
                                            "algorithm=MD5, " +
                                            "opaque=\"5caa029c403ebaf9f3333e9517f40e66\""
                                    )
                            )
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content/Type", "text/plain")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage("OK")
                            ).willSetStateTo(ACCESS_GRANTED_STATE)
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
            config.put(CONFIGURATION_ID, "default");

            List<ImmutablePair<Predicate<URI>, Proxy>> proxies = Lists.newArrayList();
            Pattern uriPattern = Pattern.compile(".*");
            Predicate<URI> predicate = uri -> uriPattern.matcher(uri.toString()).matches();
            Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(getIP(), wmRuntimeInfo.getHttpPort()));
            ImmutablePair<Predicate<URI>, Proxy> pair = new ImmutablePair<>(predicate, proxy);
            proxies.add(pair);
            URIRegexProxySelector proxySelector = new URIRegexProxySelector(proxies);
            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, proxySelector, getCompositeMeterRegistry());

            HashMap<String, List<String>> headers = Maps.newHashMap();
            headers.put(CONTENT_TYPE, Lists.newArrayList("text/plain"));
            headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
            headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
            HttpRequest httpRequest = new HttpRequest(
                    url,
                    HttpRequest.Method.POST
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
        @DisplayName("test proxy selector without authentication with two proxies")
        void test_proxy_selector_without_authentication_two_proxies() throws ExecutionException, InterruptedException {
            String bodyResponse = "{\"result\":\"pong\"}";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            //the test will call the proxy to try to forward the request, but wiremock won't relay.
            String baseUrl = "http://" + "dummy.com" + ":22222";
            String url = baseUrl + "/ping";

            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");


            List<ImmutablePair<Predicate<URI>, Proxy>> proxies = Lists.newArrayList();
            //proxy non used in this test
            Pattern uriPattern = Pattern.compile("http://toto\\.com.*");
            Predicate<URI> predicate = uri -> uriPattern.matcher(uri.toString()).matches();
            Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("111.222.888.999", 5555));
            ImmutablePair<Predicate<URI>, Proxy> pair = new ImmutablePair<>(predicate, proxy);
            proxies.add(pair);
            //proxy used in this test
            Pattern uriPattern2 = Pattern.compile("http://dummy\\.com.*");
            Predicate<URI> predicate2 = uri -> uriPattern2.matcher(uri.toString()).matches();
            Proxy proxy2 = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(getIP(), wmRuntimeInfo.getHttpPort()));
            ImmutablePair<Predicate<URI>, Proxy> pair2 = new ImmutablePair<>(predicate2, proxy2);
            proxies.add(pair2);

            URIRegexProxySelector proxySelector = new URIRegexProxySelector(proxies);

            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, proxySelector, getCompositeMeterRegistry());

            HashMap<String, List<String>> headers = Maps.newHashMap();
            headers.put(CONTENT_TYPE, Lists.newArrayList("text/plain"));
            headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
            headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
            HttpRequest httpRequest = new HttpRequest(
                    url,
                    HttpRequest.Method.POST
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

    @Nested
    class TestDnsOverHttp {
        @Test
        void test_only_activate_doh_without_bootstrap_dns_and_url() {
            //given
            Map<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(OKHTTP_DOH_ACTIVATE, "true");
            Assertions.assertThrows(IllegalStateException.class, () -> new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry()));
        }

        @Test
        void test_only_activate_doh_and_url() {
            //given
            Map<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(OKHTTP_DOH_ACTIVATE, "true");
            config.put(OKHTTP_DOH_BOOTSTRAP_DNS_HOSTS, Lists.newArrayList("1.1.1.2", "1.0.0.2"));
            Assertions.assertThrows(IllegalStateException.class, () -> new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry()));
        }

        @Test
        void test_activate_doh_and_set_url_with_bootstrap_dns() throws ExecutionException, InterruptedException {
            //given
            Map<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(OKHTTP_DOH_ACTIVATE, "true");
            config.put(OKHTTP_DOH_URL, "https://cloudflare-dns.com/dns-query");
            config.put(OKHTTP_DOH_BOOTSTRAP_DNS_HOSTS, Lists.newArrayList("1.1.1.2", "1.0.0.2"));
            OkHttpClient client = new OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());

            HttpRequest httpRequest = new HttpRequest(
                    "https://www.google.com",
                    HttpRequest.Method.GET
            );
            HttpExchange httpExchange = client.call(httpRequest, new AtomicInteger(1)).get();
            HttpResponse httpResponse = httpExchange.getHttpResponse();
            assertThat(httpResponse.getStatusCode()).isEqualTo(200);
        }

        @Test
        void test_activate_doh_and_set_url_with_bootstrap_dns_and_ipv6_activated() throws ExecutionException, InterruptedException {
            //given
            Map<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(OKHTTP_DOH_ACTIVATE, "true");
            config.put(OKHTTP_DOH_URL, "https://cloudflare-dns.com/dns-query");
            config.put(OKHTTP_DOH_BOOTSTRAP_DNS_HOSTS, Lists.newArrayList("1.1.1.2", "1.0.0.2"));
            config.put(OKHTTP_DOH_INCLUDE_IPV6, "true");
            OkHttpClient client = new OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());

            HttpRequest httpRequest = new HttpRequest(
                    "https://www.google.com",
                    HttpRequest.Method.GET
            );
            HttpExchange httpExchange = client.call(httpRequest, new AtomicInteger(1)).get();
            HttpResponse httpResponse = httpExchange.getHttpResponse();
            assertThat(httpResponse.getStatusCode()).isEqualTo(200);
        }

        @Test
        void test_activate_doh_and_set_url_with_bootstrap_dns_and_post_method() throws ExecutionException, InterruptedException {
            //given
            Map<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(OKHTTP_DOH_ACTIVATE, "true");
            config.put(OKHTTP_DOH_URL, "https://cloudflare-dns.com/dns-query");
            config.put(OKHTTP_DOH_USE_POST_METHOD, "true");
            config.put(OKHTTP_DOH_BOOTSTRAP_DNS_HOSTS, Lists.newArrayList("1.1.1.2", "1.0.0.2"));
            OkHttpClient client = new OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());

            HttpRequest httpRequest = new HttpRequest(
                    "https://www.google.com",
                    HttpRequest.Method.GET
            );
            HttpExchange httpExchange = client.call(httpRequest, new AtomicInteger(1)).get();
            HttpResponse httpResponse = httpExchange.getHttpResponse();
            assertThat(httpResponse.getStatusCode()).isEqualTo(200);
        }

        @Test
        void test_activate_doh_and_set_url_with_bootstrap_dns_and_does_not_resolve_public_addresses() throws ExecutionException, InterruptedException {
            //given
            Map<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(OKHTTP_DOH_ACTIVATE, "true");
            config.put(OKHTTP_DOH_RESOLVE_PUBLIC_ADDRESSES, "false");
            config.put(OKHTTP_DOH_URL, "https://cloudflare-dns.com/dns-query");
            config.put(OKHTTP_DOH_BOOTSTRAP_DNS_HOSTS, Lists.newArrayList("1.1.1.2", "1.0.0.2"));
            OkHttpClient client = new OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());

            HttpRequest httpRequest = new HttpRequest(
                    "https://www.toto.com",
                    HttpRequest.Method.GET
            );
            HttpExchange httpExchange = client.call(httpRequest, new AtomicInteger(1)).get();
            HttpResponse httpResponse = httpExchange.getHttpResponse();
            assertThat(httpResponse.getStatusCode()).isEqualTo(400);
            Map<String, List<String>> responseHeaders = httpResponse.getHeaders();
            assertThat(responseHeaders.get(THROWABLE_CLASS)).contains("java.net.UnknownHostException");
            assertThat(responseHeaders.get(THROWABLE_MESSAGE)).contains("public hosts not resolved");
        }

        @Test
        void test_activate_doh_and_set_url_with_bootstrap_dns_and_does_not_resolve_private_addresses() throws ExecutionException, InterruptedException {
            //given
            Map<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(OKHTTP_DOH_ACTIVATE, "true");
            config.put(OKHTTP_DOH_RESOLVE_PRIVATE_ADDRESSES, "false");
            config.put(OKHTTP_DOH_URL, "https://cloudflare-dns.com/dns-query");
            config.put(OKHTTP_DOH_BOOTSTRAP_DNS_HOSTS, Lists.newArrayList("1.1.1.2", "1.0.0.2"));
            OkHttpClient client = new OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());

            HttpRequest httpRequest = new HttpRequest(
                    "https://localhost",
                    HttpRequest.Method.GET
            );
            HttpExchange httpExchange = client.call(httpRequest, new AtomicInteger(1)).get();
            HttpResponse httpResponse = httpExchange.getHttpResponse();
            assertThat(httpResponse.getStatusCode()).isEqualTo(400);
            Map<String, List<String>> responseHeaders = httpResponse.getHeaders();
            assertThat(responseHeaders.get(THROWABLE_CLASS)).contains("java.net.UnknownHostException");
            assertThat(responseHeaders.get(THROWABLE_MESSAGE)).contains("private hosts not resolved");
        }

        @Test
        void test_activate_doh_and_set_url_with_bootstrap_dns_and_resolve_private_addresses() throws ExecutionException, InterruptedException {
            //given
            //scenario
            String scenario = "activating logging interceptor";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            String bodyResponse = "{\"result\":\"pong\"}";
            wireMock.register(WireMock.get("/ping").inScenario(scenario)
                    .whenScenarioStateIs(STARTED)
                    .willReturn(WireMock.aResponse()
                            .withBody(bodyResponse)
                            .withStatus(200)
                            .withStatusMessage("OK")
                    )
            );


            Map<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(OKHTTP_DOH_ACTIVATE, "true");
            config.put(OKHTTP_DOH_RESOLVE_PRIVATE_ADDRESSES, "true");
            config.put(OKHTTP_DOH_URL, "https://cloudflare-dns.com/dns-query");
            OkHttpClient client = new OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());

            HttpRequest httpRequest = new HttpRequest(
                    "http://127.0.0.1:" + wmHttp.getRuntimeInfo().getHttpPort() + "/ping",
                    HttpRequest.Method.GET
            );
            HttpExchange httpExchange = client.call(httpRequest, new AtomicInteger(1)).get();
            HttpResponse httpResponse = httpExchange.getHttpResponse();
            assertThat(httpResponse.getStatusCode()).isEqualTo(200);
        }


        @Test
        void test_activate_doh_and_set_url_with_bootstrap_dns_and_does_not_resolve_private_addresses_by_default() throws ExecutionException, InterruptedException {
            //given
            Map<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(OKHTTP_DOH_ACTIVATE, "true");
            config.put(OKHTTP_DOH_URL, "https://cloudflare-dns.com/dns-query");
            config.put(OKHTTP_DOH_BOOTSTRAP_DNS_HOSTS, Lists.newArrayList("1.1.1.2", "1.0.0.2"));
            OkHttpClient client = new OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());

            HttpRequest httpRequest = new HttpRequest(
                    "https://localhost",
                    HttpRequest.Method.GET
            );
            HttpExchange httpExchange = client.call(httpRequest, new AtomicInteger(1)).get();
            HttpResponse httpResponse = httpExchange.getHttpResponse();
            assertThat(httpResponse.getStatusCode()).isEqualTo(400);
            Map<String, List<String>> responseHeaders = httpResponse.getHeaders();
            assertThat(responseHeaders.get(THROWABLE_CLASS)).contains("java.net.UnknownHostException");
            assertThat(responseHeaders.get(THROWABLE_MESSAGE)).contains("private hosts not resolved");
        }

        @Test
        void test_activate_doh_and_set_url_with_dumb_bootstrap_dns() {
            //given
            Map<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(OKHTTP_DOH_ACTIVATE, "true");
            config.put(OKHTTP_DOH_URL, "https://cloudflare-dns.com/dns-query");
            config.put(OKHTTP_DOH_BOOTSTRAP_DNS_HOSTS, Lists.newArrayList("aaaaaa", "bbbbbb"));
            Assertions.assertThrows(IllegalArgumentException.class, () -> new OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry()));

        }

        @Test
        void test_activate_doh_and_set_url_but_no_system_dns() {
            //given
            Map<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(OKHTTP_DOH_ACTIVATE, "true");
            config.put(OKHTTP_DOH_URL, "https://yahoo.com");
            Assertions.assertDoesNotThrow(() -> new OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry()));


        }
    }

    @Nested
    class TestSSL {

        @Test
        void test_getTrustManagerFactory_always_trust_with_boolean_value_as_string() {

            //given
            Map<String, Object> config = Maps.newHashMap();
            config.put(HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST, "true");
            //when
            TrustManagerFactory trustManagerFactory = HttpClient.getTrustManagerFactory(config);
            //then
            assertThat(trustManagerFactory).isNotNull();
            TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
            assertThat(trustManagers).hasSize(1);
            assertThat(trustManagers[0]).isInstanceOf(X509TrustManager.class);
            X509TrustManager x509TrustManager = (X509TrustManager) trustManagers[0];
            X509Certificate dummyCertificate = new DummyX509Certificate();
            X509Certificate[] certs = new X509Certificate[]{dummyCertificate};
            Assertions.assertDoesNotThrow(() -> x509TrustManager.checkServerTrusted(certs, "RSA"));
        }

        @Test
        void test_okhtp_client_with_always_trust_with_boolean_value_as_string() throws ExecutionException, InterruptedException {

            //given
            String bodyResponse = "{\"result\":\"pong\"}";
            Map<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put(HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST, "true");
            config.put(OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION, "true");
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());
            String baseUrl = "https://" + getIP() + ":" + wmRuntimeInfo.getHttpsPort();
            String url = baseUrl + "/ping";
            HashMap<String, List<String>> headers = Maps.newHashMap();
            headers.put(CONTENT_TYPE, Lists.newArrayList("text/plain"));
            headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
            headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
            HttpRequest httpRequest = new HttpRequest(
                    url,
                    HttpRequest.Method.POST
            );
            httpRequest.setHeaders(headers);
            httpRequest.setBodyAsString("stuff");


            String scenario = "always_grant_ssl";
            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .willReturn(WireMock.aResponse()
                                    .withStatus(200)
                                    .withStatusMessage(ACCESS_GRANTED_STATE)
                                    .withBody(bodyResponse)
                            )
                    );

            //when
            HttpExchange httpExchange1 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange1.getHttpResponse().getStatusCode()).isEqualTo(200);
            HttpExchange httpExchange2 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange2.getHttpResponse().getStatusCode()).isEqualTo(200);


        }
    }

    @Nested
    class TestConnectionPool {
        @Test
        @DisplayName("test connection pool")
        void test_connection_pool() throws ExecutionException, InterruptedException {
            //given
            String bodyResponse = "{\"result\":\"pong\"}";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();

            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put("okhttp.connection.pool.max.idle.connections", 10);
            config.put("okhttp.connection.pool.keep.alive.duration", 1000);


            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());

            String baseUrl = "http://" + getIP() + ":" + wmRuntimeInfo.getHttpPort();
            String url = baseUrl + "/ping";
            HashMap<String, List<String>> headers = Maps.newHashMap();
            headers.put(CONTENT_TYPE, Lists.newArrayList("text/plain"));
            headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
            headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
            HttpRequest httpRequest = new HttpRequest(
                    url,
                    HttpRequest.Method.POST
            );
            httpRequest.setHeaders(headers);
            httpRequest.setBodyAsString("stuff");


            String scenario = "connection_pool";
            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .willReturn(WireMock.aResponse()
                                    .withStatus(200)
                                    .withStatusMessage(ACCESS_GRANTED_STATE)
                                    .withBody(bodyResponse)
                            )
                    );

            //when
            HttpExchange httpExchange1 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange1.getHttpResponse().getStatusCode()).isEqualTo(200);
            HttpExchange httpExchange2 = client.call(httpRequest, new AtomicInteger(1)).get();
            //then
            assertThat(httpExchange2.getHttpResponse().getStatusCode()).isEqualTo(200);

        }


        @Test
        @DisplayName("test connection pool with static scope")
        void test_connection_pool_with_static_scope() throws ExecutionException, InterruptedException {

            String bodyResponse = "{\"result\":\"pong\"}";
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();

            HashMap<String, Object> config = Maps.newHashMap();
            config.put(CONFIGURATION_ID, "default");
            config.put("okhttp.connection.pool.scope", "static");
            config.put("okhttp.connection.pool.max.idle.connections", 10);
            config.put("okhttp.connection.pool.keep.alive.duration", 1000);

            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client = new io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient(config, null, new Random(), null, null, getCompositeMeterRegistry());

            HashMap<String, Object> config2 = Maps.newHashMap();
            config2.put(CONFIGURATION_ID, "default");
            config2.put("okhttp.connection.pool.scope", "static");
            config2.put("okhttp.connection.pool.max.idle.connections", 10);
            config2.put("okhttp.connection.pool.keep.alive.duration", 1000);


            io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient client2 = new OkHttpClient(config2, null, new Random(), null, null, getCompositeMeterRegistry());

            String baseUrl = "http://" + getIP() + ":" + wmRuntimeInfo.getHttpPort();
            String url = baseUrl + "/ping";
            HashMap<String, List<String>> headers = Maps.newHashMap();
            headers.put(CONTENT_TYPE, Lists.newArrayList("text/plain"));
            headers.put("X-Correlation-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-754880687822"));
            headers.put("X-Request-ID", Lists.newArrayList("e6de70d1-f222-46e8-b755-11111"));
            HttpRequest httpRequest = new HttpRequest(
                    url,
                    HttpRequest.Method.POST
            );
            httpRequest.setHeaders(headers);
            httpRequest.setBodyAsString("stuff");


            String scenario = "connection_pool";
            wireMock
                    .register(WireMock.post("/ping").inScenario(scenario)
                            .willReturn(WireMock.aResponse()
                                    .withStatus(200)
                                    .withStatusMessage(ACCESS_GRANTED_STATE)
                                    .withBody(bodyResponse)
                            )
                    );

            HttpExchange httpExchange1 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange1.getHttpResponse().getStatusCode()).isEqualTo(200);
            HttpExchange httpExchange2 = client.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange2.getHttpResponse().getStatusCode()).isEqualTo(200);
            HttpExchange httpExchange3 = client2.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange3.getHttpResponse().getStatusCode()).isEqualTo(200);
            HttpExchange httpExchange4 = client2.call(httpRequest, new AtomicInteger(1)).get();
            assertThat(httpExchange4.getHttpResponse().getStatusCode()).isEqualTo(200);

            assertThat(client.getInternalClient().connectionPool()).isEqualTo(client2.getInternalClient().connectionPool());
        }
    }
}
