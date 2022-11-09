package com.github.clescot.kafka.connect.http.sink.client;


import com.github.clescot.kafka.connect.http.source.HttpExchange;
import com.google.common.base.Stopwatch;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.clescot.kafka.connect.http.sink.client.HttpClient.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.asynchttpclient.util.HttpConstants.Methods.PUT;
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
            HttpClient httpClient = new HttpClient(asyncHttpClient);
            httpClient.buildHttpExchange(null,
                    null,
                    null,
                    Maps.newHashMap(),
                    null,
                    null,
                    Maps.newHashMap(),
                    null,
                    -1,
                    null,
                    Stopwatch.createUnstarted(),
                    OffsetDateTime.now(ZoneId.of(HttpClient.UTC_ZONE_ID)),
                    new AtomicInteger(2),
                    SUCCESS
            );
        }

        @Test(expected = NullPointerException.class)
        public void test_content_is_null() {
            HttpClient httpClient = new HttpClient(asyncHttpClient);
            httpClient.buildHttpExchange(null,
                    null,
                    null,
                    Maps.newHashMap(),
                    null,
                    null,
                    Maps.newHashMap(),
                    null,
                    200,
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
            HttpClient httpClient = new HttpClient(asyncHttpClient);
            httpClient.buildHttpExchange("fsqdfsdf",
                    null,
                    null,
                    null,
                    null,
                    null,
                    Maps.newHashMap(),
                    null,
                    200,
                    "",
                    Stopwatch.createUnstarted(),
                    OffsetDateTime.now(ZoneId.of(HttpClient.UTC_ZONE_ID)),
                    new AtomicInteger(2),
                    SUCCESS);
        }

        @Test(expected = IllegalStateException.class)
        public void test_response_code_is_lower_than_0() {
            HttpClient httpClient = new HttpClient(asyncHttpClient);
            httpClient.buildHttpExchange("fsqdfsdf",
                    "sdfsfdsf",
                    "http://stuff.com/sfsfds",
                    Maps.newHashMap(),
                    PUT,
                    "fake body",
                    Maps.newHashMap(),
                    "fake response body",
                    -1,
                    "",
                    Stopwatch.createUnstarted(),
                    OffsetDateTime.now(ZoneId.of(HttpClient.UTC_ZONE_ID)),
                    new AtomicInteger(2),
                    SUCCESS);
        }


        @Test
        public void test_nominal_case() {
            HashMap<String,
                    String> vars = Maps.newHashMap();
            HttpClient httpClient = new HttpClient(asyncHttpClient);
            httpClient.buildHttpExchange("fsqdfsdf",
                    "requestId",
                    "requestUri",
                    Maps.newHashMap(),
                    "PUT",
                    "",
                    Maps.newHashMap(),
                    "",
                    200,
                    "" +"",
                    Stopwatch.createUnstarted(),
                    OffsetDateTime.now(ZoneId.of(HttpClient.UTC_ZONE_ID)),
                    new AtomicInteger(2),
                    SUCCESS);
        }
    }

    public static class Test_callOnceWs {


        @Test
        public void test_nominal_case() throws ExecutionException, InterruptedException {
            AsyncHttpClient asyncHttpClient = mock(AsyncHttpClient.class);
            ListenableFuture<Response> listener = mock(ListenableFuture.class);
            ListenableFuture<Object> listenerObject = mock(ListenableFuture.class);
            Response response = mock(Response.class);
            Uri uri = new Uri("http", null, "fakeHost", 8080, "/toto", "param1=3", "#4");
            when(response.getUri()).thenReturn(uri);

            when(response.getResponseBody()).thenReturn("body");
            when(response.getStatusCode()).thenReturn(200);
            when(response.getStatusText()).thenReturn("OK");

            when(listener.get()).thenReturn(response);
            when(listenerObject.get()).thenReturn(response);
            when(asyncHttpClient.executeRequest(any(Request.class))).thenReturn(listener);
            when(asyncHttpClient.executeRequest(any(Request.class), any())).thenReturn(listenerObject);
            HttpClient httpClient = new HttpClient(asyncHttpClient);
            HashMap<String, String> wsProperties = Maps.newHashMap();
            wsProperties.put("url", "http://localhost:8089");
            wsProperties.put("method", "PUT");
            wsProperties.put("correlation-id", "sgsmr,sdfgsjjmsldf");
            wsProperties.put(HEADER_X_CORRELATION_ID, "sdfd-7899-8921");
            wsProperties.put(HEADER_X_REQUEST_ID, "sdfd-dfdf-8921");
            HttpExchange httpExchange = httpClient.callOnceWs("requestId", wsProperties, "body", new AtomicInteger(2));
            assertThat(httpExchange).isNotNull();
        }

        @Test
        public void test_any_positive_int_success_code_lower_than_500() throws ExecutionException, InterruptedException {
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
            HttpClient httpClient = new HttpClient(asyncHttpClient);
            HashMap<String, String> wsProperties = Maps.newHashMap();
            wsProperties.put("url", "http://localhost:8089");
            wsProperties.put("method", "PUT");
            wsProperties.put("correlation-id", "sgsmr,sdfgsjjmsldf");
            wsProperties.put("success-code", "^\\d+$");
            wsProperties.put(HEADER_X_CORRELATION_ID, "sdfd-7899-8921");
            wsProperties.put(HEADER_X_REQUEST_ID, "sdfd-dfdf-8921");
            HttpExchange httpExchange = httpClient.callOnceWs("requestId", wsProperties, "body", new AtomicInteger(2));
            assertThat(httpExchange).isNotNull();
        }


        @Test(expected = HttpException.class)
        public void test_failure_server_side() throws ExecutionException, InterruptedException {
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
            HttpClient httpClient = new HttpClient(asyncHttpClient);
            HashMap<String, String> wsProperties = Maps.newHashMap();
            wsProperties.put("url", "http://localhost:8089");
            wsProperties.put("method", "PUT");
            wsProperties.put("correlation-id", "sgsmr,sdfgsjjmsldf");
            wsProperties.put("success-code", "^[1-4][0-9][0-9]$");
            wsProperties.put(HEADER_X_CORRELATION_ID, "sdfd-7899-8921");
            wsProperties.put(HEADER_X_REQUEST_ID, "sdfd-dfdf-8921");
            HttpExchange httpExchange = httpClient.callOnceWs("requestId", wsProperties, "body", new AtomicInteger(2));
            assertThat(httpExchange).isNotNull();
        }

        @Test
        public void test_failure_client_side() throws ExecutionException, InterruptedException {
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
            HttpClient httpClient = new HttpClient(asyncHttpClient);
            HashMap<String, String> wsProperties = Maps.newHashMap();
            wsProperties.put("url", "http://localhost:8089");
            wsProperties.put("method", "PUT");
            wsProperties.put("correlation-id", "sgsmr,sdfgsjjmsldf");
            wsProperties.put("success-code", "^[1-4][0-9][0-9]$");
            wsProperties.put(HEADER_X_CORRELATION_ID, "sdfd-7899-8921");
            wsProperties.put(HEADER_X_REQUEST_ID, "sdfd-dfdf-8921");
            HttpExchange httpExchange = httpClient.callOnceWs("requestId", wsProperties, "body", new AtomicInteger(2));
            assertThat(httpExchange).isNotNull();
        }

    }

}