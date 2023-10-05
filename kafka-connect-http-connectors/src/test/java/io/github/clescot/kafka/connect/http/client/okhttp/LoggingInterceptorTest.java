package io.github.clescot.kafka.connect.http.client.okhttp;

import com.google.common.collect.Lists;
import okhttp3.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LoggingInterceptorTest {
    private final Logger LOGGER = LoggerFactory.getLogger(LoggingInterceptorTest.class);
    private AutoCloseable closeable;
    @Mock
    Interceptor.Chain chain;
    @Mock
    Request request;
    @Mock
    Response response;
    @Mock
    Connection connection;
    @Test
    void test_intercept() throws IOException {
        //given
        when(chain.request()).thenReturn(request);
        String connectionAsString=
                "\"Connection{stuff.com:443,\" \n" +
                "  proxy=\"none\" \n" +
                "  hostAddress=241.212.235.23\" \n" +
                "  cipherSuite=\"none\" \n" +
                "  protocol=http/1.1}\"";
        when(connection.toString()).thenReturn(connectionAsString);
        when(chain.connection()).thenReturn(connection);
        when(chain.proceed(request)).thenReturn(response);
        HttpUrl httpUrl = new HttpUrl("https","","","stuff.com",443, Lists.newArrayList(),null,"/","https://stuff.com");
        when(request.url()).thenReturn(httpUrl);
        Headers.Builder headersRequestBuilder = new Headers.Builder();
        headersRequestBuilder.add("Content-Type","application/json");
        when(request.headers()).thenReturn(headersRequestBuilder.build());
        when(response.request()).thenReturn(request);
        when(response.code()).thenReturn(200);
        when(response.message()).thenReturn("OK");
        Headers.Builder headersResponseBuilder = new Headers.Builder();
        headersResponseBuilder.add("Content-Type","application/json");
        when(response.headers()).thenReturn(headersResponseBuilder.build());
        LoggingInterceptor loggingInterceptor = new LoggingInterceptor();
        //when
        Response myResponse = loggingInterceptor.intercept(chain);
        //then
        assertThat(myResponse).isNotNull();
    }

}