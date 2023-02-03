package io.github.clescot.kafka.connect.http.sink.client;

import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.sink.HttpSinkTaskTest;
import io.github.clescot.kafka.connect.http.sink.client.ahc.AHCHttpClient;
import io.github.clescot.kafka.connect.http.sink.client.okhttp.OkHttpClientFactory;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.asynchttpclient.uri.Uri;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.net.ssl.TrustManagerFactory;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkTask.HEADER_X_CORRELATION_ID;
import static io.github.clescot.kafka.connect.http.sink.HttpSinkTask.HEADER_X_REQUEST_ID;
import static io.github.clescot.kafka.connect.http.sink.client.ahc.AHCHttpClient.SUCCESS;
import static org.assertj.core.api.Assertions.assertThat;

public class HttpClientTest {

        private AsyncHttpClient asyncHttpClient;

        @BeforeEach
        public void setUp() {
            asyncHttpClient = Mockito.mock(AsyncHttpClient.class);
        }

        @Test
        public void build_HttpExchange_test_all_null() {
            AHCHttpClient httpClient = new AHCHttpClient(asyncHttpClient);
            org.junit.jupiter.api.Assertions.assertThrows(NullPointerException.class,()->
            httpClient.buildHttpExchange(null,
                    null,
                    Stopwatch.createUnstarted(),
                    OffsetDateTime.now(ZoneId.of(AHCHttpClient.UTC_ZONE_ID)),
                    new AtomicInteger(2),
                    SUCCESS
            ));
        }



        @Test
        public void build_HttpExchange_test_message_is_null() {
            AHCHttpClient httpClient = new AHCHttpClient(asyncHttpClient);
            org.junit.jupiter.api.Assertions.assertThrows(NullPointerException.class,()->
            httpClient.buildHttpExchange(null,
                    getDummyHttpResponse(200),
                    Stopwatch.createUnstarted(),
                    OffsetDateTime.now(ZoneId.of(AHCHttpClient.UTC_ZONE_ID)),
                    new AtomicInteger(2),
                    SUCCESS));
        }

        @Test
        public void build_HttpExchange_test_response_code_is_lower_than_0() {
            AHCHttpClient httpClient = new AHCHttpClient(asyncHttpClient);
            org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,()->httpClient.buildHttpExchange(getDummyHttpRequest(),
                    getDummyHttpResponse(-12),
                    Stopwatch.createUnstarted(),
                    OffsetDateTime.now(ZoneId.of(AHCHttpClient.UTC_ZONE_ID)),
                    new AtomicInteger(2),
                    SUCCESS));
        }


        @Test
        public void build_HttpExchange_test_nominal_case() {
            HashMap<String,
                    String> vars = Maps.newHashMap();
            AHCHttpClient httpClient = new AHCHttpClient(asyncHttpClient);
            httpClient.buildHttpExchange(getDummyHttpRequest(),
                   getDummyHttpResponse(200),
                    Stopwatch.createUnstarted(),
                    OffsetDateTime.now(ZoneId.of(AHCHttpClient.UTC_ZONE_ID)),
                    new AtomicInteger(2),
                    SUCCESS);
        }

        private HttpResponse getDummyHttpResponse(int statusCode) {
            HttpResponse httpResponse = new HttpResponse(statusCode,"OK");
            httpResponse.setResponseBody("my response");
            Map<String,List<String>> headers = Maps.newHashMap();
            headers.put("Content-Type",Lists.newArrayList("application/json"));
            headers.put("X-stuff",Lists.newArrayList("foo"));
            httpResponse.setResponseHeaders(headers);
            return httpResponse;
        }


        @Test
        public void call_test_nominal_case() throws ExecutionException, InterruptedException {

            //given
            AsyncHttpClient asyncHttpClient = Mockito.mock(AsyncHttpClient.class);
            ListenableFuture<Response> listener = Mockito.mock(ListenableFuture.class);
            ListenableFuture<Object> listenerObject = Mockito.mock(ListenableFuture.class);
            Response response = Mockito.mock(Response.class);

            Mockito.when(response.getResponseBody()).thenReturn("body");
            int statusCode = 200;
            Mockito.when(response.getStatusCode()).thenReturn(statusCode);
            String statusMessage = "OK";
            Mockito.when(response.getStatusText()).thenReturn(statusMessage);

            Mockito.when(listener.get()).thenReturn(response);
            Mockito.when(listenerObject.get()).thenReturn(response);
            Mockito.when(asyncHttpClient.executeRequest(ArgumentMatchers.any(Request.class))).thenReturn(listener);
            Mockito.when(asyncHttpClient.executeRequest(ArgumentMatchers.any(Request.class), ArgumentMatchers.any())).thenReturn(listenerObject);
            AHCHttpClient httpClient = new AHCHttpClient(asyncHttpClient);

            //when
            HttpExchange httpExchange = httpClient.call(getDummyHttpRequest(), new AtomicInteger(2));

            //then
            assertThat(httpExchange).isNotNull();
            assertThat(httpExchange.getHttpRequest().getUrl()).isEqualTo("http://localhost:8089");
            assertThat(httpExchange.getHttpResponse().getStatusCode()).isEqualTo(statusCode);
            assertThat(httpExchange.getHttpResponse().getStatusMessage()).isEqualTo(statusMessage);
        }

        @Test
        public void call_test_any_positive_int_success_code_lower_than_500() throws ExecutionException, InterruptedException {
            //given
            AsyncHttpClient asyncHttpClient = Mockito.mock(AsyncHttpClient.class);
            ListenableFuture<Response> listener = Mockito.mock(ListenableFuture.class);
            ListenableFuture<Object> listenerObject = Mockito.mock(ListenableFuture.class);
            Response response = Mockito.mock(Response.class);
            Uri uri = new Uri("http", null, "fakeHost", 8080, "/toto", "param1=3", "#4");
            Mockito.when(response.getUri()).thenReturn(uri);

            Mockito.when(response.getResponseBody()).thenReturn("body");
            Mockito.when(response.getStatusCode()).thenReturn(404);
            Mockito.when(response.getStatusText()).thenReturn("OK");
            Mockito.when(listener.get()).thenReturn(response);
            Mockito.when(listenerObject.get()).thenReturn(response);
            Mockito.when(asyncHttpClient.executeRequest(ArgumentMatchers.any(Request.class))).thenReturn(listener);
            Mockito.when(asyncHttpClient.executeRequest(ArgumentMatchers.any(Request.class), ArgumentMatchers.any())).thenReturn(listenerObject);
            HashMap<String, String> vars = Maps.newHashMap();
            AHCHttpClient httpClient = new AHCHttpClient(asyncHttpClient);
            //when
            HttpExchange httpExchange = httpClient.call(getDummyHttpRequest(), new AtomicInteger(2));
            //then
            assertThat(httpExchange).isNotNull();
        }


        @Test
        public void call_test_failure_server_side() throws ExecutionException, InterruptedException {
            //given
            AsyncHttpClient asyncHttpClient = Mockito.mock(AsyncHttpClient.class);
            ListenableFuture<Response> listener = Mockito.mock(ListenableFuture.class);
            ListenableFuture<Object> listenerObject = Mockito.mock(ListenableFuture.class);
            Response response = Mockito.mock(Response.class);
            Mockito.when(response.getResponseBody()).thenReturn("body");
            Mockito.when(response.getStatusCode()).thenReturn(500);
            Mockito.when(response.getStatusText()).thenReturn("OK");
            Mockito.when(listener.get()).thenReturn(response);
            Mockito.when(listenerObject.get()).thenReturn(response);
            Mockito.when(asyncHttpClient.executeRequest(ArgumentMatchers.any(Request.class))).thenReturn(listener);
            Mockito.when(asyncHttpClient.executeRequest(ArgumentMatchers.any(Request.class), ArgumentMatchers.any())).thenReturn(listenerObject);
            AHCHttpClient httpClient = new AHCHttpClient(asyncHttpClient);

            //when
            HttpExchange httpExchange = httpClient.call(getDummyHttpRequest(), new AtomicInteger(2));
            //then
            assertThat(httpExchange).isNotNull();
        }

        @Test
        public void call_test_failure_client_side() throws ExecutionException, InterruptedException {
            //given
            AsyncHttpClient asyncHttpClient = Mockito.mock(AsyncHttpClient.class);
            ListenableFuture<Response> listener = Mockito.mock(ListenableFuture.class);
            ListenableFuture<Object> listenerObject = Mockito.mock(ListenableFuture.class);
            Response response = Mockito.mock(Response.class);
            Uri uri = new Uri("http", null, "fakeHost", 8080, "/toto", "param1=3", "#4");
            Mockito.when(response.getUri()).thenReturn(uri);

            Mockito.when(response.getResponseBody()).thenReturn("body");
            Mockito.when(response.getStatusCode()).thenReturn(400);
            Mockito.when(response.getStatusText()).thenReturn("OK");

            Mockito.when(listener.get()).thenReturn(response);
            Mockito.when(listenerObject.get()).thenReturn(response);
            Mockito.when(asyncHttpClient.executeRequest(ArgumentMatchers.any(Request.class))).thenReturn(listener);
            Mockito.when(asyncHttpClient.executeRequest(ArgumentMatchers.any(Request.class), ArgumentMatchers.any())).thenReturn(listenerObject);
            HashMap<String, String> vars = Maps.newHashMap();
            AHCHttpClient httpClient = new AHCHttpClient(asyncHttpClient);
            //when
            HttpExchange httpExchange = httpClient.call(getDummyHttpRequest(), new AtomicInteger(2));
            //then
            assertThat(httpExchange).isNotNull();
        }

        @Test
        public void test_build_http_request_nominal_case(){
            //given
            AsyncHttpClient asyncHttpClient = Mockito.mock(AsyncHttpClient.class);
            AHCHttpClient httpClient = new AHCHttpClient(asyncHttpClient);

            //when
            HttpRequest httpRequest = getDummyHttpRequest();
            Request request = httpClient.buildRequest(httpRequest);

            //then
            assertThat(request.getUrl()).isEqualTo(httpRequest.getUrl());
            assertThat(request.getMethod()).isEqualTo(httpRequest.getMethod());
        }


    private static HttpRequest getDummyHttpRequest(){
        return getDummyHttpRequest("{\"param\":\"name\"}");
    }

    private static HttpRequest getDummyHttpRequest(String body){
        HashMap<String, List<String>> headers = Maps.newHashMap();
        headers.put("Content-Type", Lists.newArrayList("application/json"));
        headers.put("X-Stuff", Lists.newArrayList("dummy stuff"));
        HttpRequest httpRequest = new HttpRequest(
                "http://localhost:8089",
                "GET",
                "STRING");
        httpRequest.setHeaders(headers);
        httpRequest.setBodyAsString(body);
        httpRequest.getHeaders().put(HEADER_X_CORRELATION_ID,Lists.newArrayList("45-66-33"));
        httpRequest.getHeaders().put(HEADER_X_REQUEST_ID,Lists.newArrayList("77-3333-11"));
        return httpRequest;
    }

    @Test
    public void test_getTrustManagerFactory_jks_nominal_case(){

        //given
        String truststorePath = Thread.currentThread().getContextClassLoader().getResource(HttpSinkTaskTest.CLIENT_TRUSTSTORE_JKS_FILENAME).getPath();
        String password = HttpSinkTaskTest.CLIENT_TRUSTSTORE_JKS_PASSWORD;
        OkHttpClientFactory httpClientFactory = new OkHttpClientFactory();

        //when
        TrustManagerFactory trustManagerFactory = HttpClient.getTrustManagerFactory(truststorePath, password.toCharArray(), HttpSinkTaskTest.JKS_STORE_TYPE, HttpSinkTaskTest.TRUSTSTORE_PKIX_ALGORITHM);
        //then
        assertThat(trustManagerFactory).isNotNull();
        assertThat(trustManagerFactory.getTrustManagers()).hasSize(1);

    }



}