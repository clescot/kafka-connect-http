package com.github.clescot.kafka.connect.http.sink.client;


import com.github.clescot.kafka.connect.http.HttpRequest;
import com.github.clescot.kafka.connect.http.HttpExchange;
import com.github.clescot.kafka.connect.http.HttpResponse;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.asynchttpclient.uri.Uri;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.clescot.kafka.connect.http.sink.client.HttpClient.SUCCESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Enclosed.class)
public class HttpClientTest {

    public static class Test_setHttpExchange {
        private AsyncHttpClient asyncHttpClient;

        @Before
        public void setUp() {
            asyncHttpClient = mock(AsyncHttpClient.class);
        }

        @Test(expected = NullPointerException.class)
        public void test_all_null() {
            HttpClient httpClient = new HttpClient(asyncHttpClient, Optional.empty());
            httpClient.buildHttpExchange(null,
                    null,
                    Stopwatch.createUnstarted(),
                    OffsetDateTime.now(ZoneId.of(HttpClient.UTC_ZONE_ID)),
                    new AtomicInteger(2),
                    SUCCESS
            );
        }



        @Test(expected = NullPointerException.class)
        public void test_message_is_null() {
            HashMap<String,
                    String> vars = Maps.newHashMap();
            HttpClient httpClient = new HttpClient(asyncHttpClient,Optional.empty());
            httpClient.buildHttpExchange(null,
                    getDummyHttpResponse(200),
                    Stopwatch.createUnstarted(),
                    OffsetDateTime.now(ZoneId.of(HttpClient.UTC_ZONE_ID)),
                    new AtomicInteger(2),
                    SUCCESS);
        }

        @Test(expected = IllegalArgumentException.class)
        public void test_response_code_is_lower_than_0() {
            HttpClient httpClient = new HttpClient(asyncHttpClient,Optional.empty());
            httpClient.buildHttpExchange(getDummyHttpRequest(),
                   getDummyHttpResponse(-12),
                    Stopwatch.createUnstarted(),
                    OffsetDateTime.now(ZoneId.of(HttpClient.UTC_ZONE_ID)),
                    new AtomicInteger(2),
                    SUCCESS);
        }


        @Test
        public void test_nominal_case() {
            HashMap<String,
                    String> vars = Maps.newHashMap();
            HttpClient httpClient = new HttpClient(asyncHttpClient,Optional.empty());
            httpClient.buildHttpExchange(getDummyHttpRequest(),
                   getDummyHttpResponse(200),
                    Stopwatch.createUnstarted(),
                    OffsetDateTime.now(ZoneId.of(HttpClient.UTC_ZONE_ID)),
                    new AtomicInteger(2),
                    SUCCESS);
        }

        private HttpResponse getDummyHttpResponse(int statusCode) {
            HttpResponse httpResponse = new HttpResponse(statusCode,"OK","my response");
            Map<String,List<String>> headers = Maps.newHashMap();
            headers.put("Content-Type",Lists.newArrayList("application/json"));
            headers.put("X-stuff",Lists.newArrayList("foo"));
            httpResponse.setResponseHeaders(headers);
            return httpResponse;
        }
    }

    public static class Test_callOnceWs {


        @Test
        public void test_nominal_case() throws ExecutionException, InterruptedException {

            //given
            AsyncHttpClient asyncHttpClient = mock(AsyncHttpClient.class);
            ListenableFuture<Response> listener = mock(ListenableFuture.class);
            ListenableFuture<Object> listenerObject = mock(ListenableFuture.class);
            Response response = mock(Response.class);

            when(response.getResponseBody()).thenReturn("body");
            int statusCode = 200;
            when(response.getStatusCode()).thenReturn(statusCode);
            String statusMessage = "OK";
            when(response.getStatusText()).thenReturn(statusMessage);

            when(listener.get()).thenReturn(response);
            when(listenerObject.get()).thenReturn(response);
            when(asyncHttpClient.executeRequest(any(Request.class))).thenReturn(listener);
            when(asyncHttpClient.executeRequest(any(Request.class), any())).thenReturn(listenerObject);
            HttpClient httpClient = new HttpClient(asyncHttpClient,Optional.empty());

            //when
            HttpExchange httpExchange = httpClient.callOnceWs(getDummyHttpRequest(), new AtomicInteger(2));

            //then
            assertThat(httpExchange).isNotNull();
            assertThat(httpExchange.getHttpRequest().getUrl()).isEqualTo("http://localhost:8089");
            assertThat(httpExchange.getHttpResponse().getStatusCode()).isEqualTo(statusCode);
            assertThat(httpExchange.getHttpResponse().getStatusMessage()).isEqualTo(statusMessage);
        }

        @Test
        public void test_any_positive_int_success_code_lower_than_500() throws ExecutionException, InterruptedException {
            //given
            AsyncHttpClient asyncHttpClient = mock(AsyncHttpClient.class);
            ListenableFuture<Response> listener = mock(ListenableFuture.class);
            ListenableFuture<Object> listenerObject = mock(ListenableFuture.class);
            Response response = mock(Response.class);
            Uri uri = new Uri("http", null, "fakeHost", 8080, "/toto", "param1=3", "#4");
            when(response.getUri()).thenReturn(uri);

            when(response.getResponseBody()).thenReturn("body");
            when(response.getStatusCode()).thenReturn(404);
            when(response.getStatusText()).thenReturn("OK");
            when(listener.get()).thenReturn(response);
            when(listenerObject.get()).thenReturn(response);
            when(asyncHttpClient.executeRequest(any(Request.class))).thenReturn(listener);
            when(asyncHttpClient.executeRequest(any(Request.class), any())).thenReturn(listenerObject);
            HashMap<String, String> vars = Maps.newHashMap();
            HttpClient httpClient = new HttpClient(asyncHttpClient,Optional.empty());
            //when
            HttpExchange httpExchange = httpClient.callOnceWs(getDummyHttpRequest(), new AtomicInteger(2));
            //then
            assertThat(httpExchange).isNotNull();
        }


        @Test(expected = HttpException.class)
        public void test_failure_server_side() throws ExecutionException, InterruptedException {
            //given
            AsyncHttpClient asyncHttpClient = mock(AsyncHttpClient.class);
            ListenableFuture<Response> listener = mock(ListenableFuture.class);
            ListenableFuture<Object> listenerObject = mock(ListenableFuture.class);
            Response response = mock(Response.class);
            when(response.getResponseBody()).thenReturn("body");
            when(response.getStatusCode()).thenReturn(500);
            when(response.getStatusText()).thenReturn("OK");
            when(listener.get()).thenReturn(response);
            when(listenerObject.get()).thenReturn(response);
            when(asyncHttpClient.executeRequest(any(Request.class))).thenReturn(listener);
            when(asyncHttpClient.executeRequest(any(Request.class), any())).thenReturn(listenerObject);
            HashMap<String, String> vars = Maps.newHashMap();
            HttpClient httpClient = new HttpClient(asyncHttpClient,Optional.empty());
            //when
            HttpExchange httpExchange = httpClient.callOnceWs(getDummyHttpRequest(), new AtomicInteger(2));
            //then
            assertThat(httpExchange).isNotNull();
        }

        @Test
        public void test_failure_client_side() throws ExecutionException, InterruptedException {
            //given
            AsyncHttpClient asyncHttpClient = mock(AsyncHttpClient.class);
            ListenableFuture<Response> listener = mock(ListenableFuture.class);
            ListenableFuture<Object> listenerObject = mock(ListenableFuture.class);
            Response response = mock(Response.class);
            Uri uri = new Uri("http", null, "fakeHost", 8080, "/toto", "param1=3", "#4");
            when(response.getUri()).thenReturn(uri);

            when(response.getResponseBody()).thenReturn("body");
            when(response.getStatusCode()).thenReturn(400);
            when(response.getStatusText()).thenReturn("OK");

            when(listener.get()).thenReturn(response);
            when(listenerObject.get()).thenReturn(response);
            when(asyncHttpClient.executeRequest(any(Request.class))).thenReturn(listener);
            when(asyncHttpClient.executeRequest(any(Request.class), any())).thenReturn(listenerObject);
            HashMap<String, String> vars = Maps.newHashMap();
            HttpClient httpClient = new HttpClient(asyncHttpClient,Optional.empty());
            //when
            HttpExchange httpExchange = httpClient.callOnceWs(getDummyHttpRequest(), new AtomicInteger(2));
            //then
            assertThat(httpExchange).isNotNull();
        }

        @Test
        public void test_build_http_request_nominal_case(){
            //given
            AsyncHttpClient asyncHttpClient = mock(AsyncHttpClient.class);
            HttpClient httpClient = new HttpClient(asyncHttpClient,Optional.empty());

            //when
            HttpRequest httpRequest = getDummyHttpRequest();
            Request request = httpClient.buildRequest(httpRequest);

            //then
            assertThat(request.getUrl()).isEqualTo(httpRequest.getUrl());
            assertThat(request.getMethod()).isEqualTo(httpRequest.getMethod());
        }




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
                "STRING",
                body,
                null,
                null);
        httpRequest.setHeaders(headers);
        httpRequest.setRetries(10L);
        httpRequest.setRetryDelayInMs(2500L);
        httpRequest.setRetryMaxDelayInMs(7000L);
        httpRequest.setRetryDelayFactor(2d);
        httpRequest.setCorrelationId("45-66-33");
        httpRequest.setRequestId("77-3333-11");
        return httpRequest;
    }



}