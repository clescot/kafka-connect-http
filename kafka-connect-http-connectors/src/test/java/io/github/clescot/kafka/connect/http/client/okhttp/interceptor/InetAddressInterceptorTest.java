package io.github.clescot.kafka.connect.http.client.okhttp.interceptor;

import okhttp3.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InetAddressInterceptorTest {
    private final Logger LOGGER = LoggerFactory.getLogger(InetAddressInterceptorTest.class);

    @Mock
    Interceptor.Chain chain;
    Request request;
    Response response;
    @Mock
    Connection connection;
    @Mock
    Socket socket;
    @Mock
    InetAddress inetAddress;
    @Test
    void test_intercept() throws IOException {
        //given
        request = new Request.Builder()
                .url("http://example.com")
                .build();
        response = new Response.Builder()
                .request(request)
                .protocol(Protocol.HTTP_1_1)
                .code(200)
                .message("OK")
                .body(ResponseBody.create("", MediaType.get("text/plain")))
                .build();
        when(chain.request()).thenReturn(request);
        when(chain.connection()).thenReturn(connection);
        when(chain.proceed(any(Request.class))).thenReturn(response);
        when(connection.socket()).thenReturn(socket);
        when(socket.getInetAddress()).thenReturn(inetAddress);
        when(inetAddress.getHostAddress()).thenReturn("hostAddress");
        when(inetAddress.getHostName()).thenReturn("hostName");
        when(inetAddress.getCanonicalHostName()).thenReturn("canonicalHostName");

        InetAddressInterceptor inetAddressInterceptor = new InetAddressInterceptor();

        //when
        Response myResponse = inetAddressInterceptor.intercept(chain);
        //then
        assertThat(myResponse).isNotNull();
    }
}