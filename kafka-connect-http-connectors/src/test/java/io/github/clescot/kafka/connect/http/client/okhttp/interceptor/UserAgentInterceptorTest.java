package io.github.clescot.kafka.connect.http.client.okhttp.interceptor;


import okhttp3.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class UserAgentInterceptorTest {

    @Mock
    Interceptor.Chain chain;
    @Captor
    ArgumentCaptor<Request> requestCaptor;
    @Test
    void test_undefined_user_agent() throws IOException {
        Headers.Builder headersBuilder = new Headers.Builder();
        Request.Builder builder = new Request.Builder();
        builder.url("https://test.com");
        builder.headers(headersBuilder.build());
        Request modifiedRequest = builder.build();
        when(chain.request()).thenReturn(modifiedRequest);
        //given
        UserAgentInterceptor userAgentInterceptor = new UserAgentInterceptor("modified");
        Response.Builder responseBuilder = new Response.Builder();
        responseBuilder.code(200);
        responseBuilder.message("OK");
        responseBuilder.protocol(Protocol.HTTP_1_1);
        responseBuilder.request(modifiedRequest);
        when(chain.proceed(any(Request.class))).thenReturn(responseBuilder.build());
        //when
        userAgentInterceptor.intercept(chain);
        //then
        verify(chain).proceed(requestCaptor.capture());
        Request value = requestCaptor.getValue();
        assertThat(value.headers("User-Agent")).hasSize(1);
        assertThat(value.headers("User-Agent")).contains("modified");
    }

    @Test
    void test_null_user_agent() {
        //when
        Assertions.assertThrows(NullPointerException.class,()->new UserAgentInterceptor(null));

    }

    @Test
    void test_already_defined_user_agent() throws IOException {
        Headers.Builder headersBuilder = new Headers.Builder();
        headersBuilder.add("User-Agent","okhttp/4.11.0");
        Request.Builder builder = new Request.Builder();
        builder.url("https://test.com");
        builder.headers(headersBuilder.build());
        Request modifiedRequest = builder.build();
        when(chain.request()).thenReturn(modifiedRequest);
        //given
        UserAgentInterceptor userAgentInterceptor = new UserAgentInterceptor("modified");
        Response.Builder responseBuilder = new Response.Builder();
        responseBuilder.code(200);
        responseBuilder.message("OK");
        responseBuilder.protocol(Protocol.HTTP_1_1);
        responseBuilder.request(modifiedRequest);
        when(chain.proceed(any(Request.class))).thenReturn(responseBuilder.build());
        //when
        userAgentInterceptor.intercept(chain);
        //then
        verify(chain).proceed(requestCaptor.capture());
        Request value = requestCaptor.getValue();
        assertThat(value.headers("User-Agent")).hasSize(1);
        assertThat(value.headers("User-Agent")).contains("modified");
    }
}