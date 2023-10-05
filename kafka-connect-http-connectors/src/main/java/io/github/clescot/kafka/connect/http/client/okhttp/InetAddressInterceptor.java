package io.github.clescot.kafka.connect.http.client.okhttp;

import okhttp3.Headers;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

public class InetAddressInterceptor implements Interceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(InetAddressInterceptor.class);

    @NotNull
    @Override
    public Response intercept(@NotNull Chain chain) throws IOException {
        Request request = chain.request();
        Response response = chain.proceed(request);

        Socket socket = chain.connection().socket();
        if (socket != null) {
            InetAddress inetAddress = socket.getInetAddress();

            Headers.Builder okHeadersBuilder = new Headers.Builder();
            okHeadersBuilder.addAll(response.headers());

            String hostAddress = inetAddress.getHostAddress();
            LOGGER.debug("hostAddress: '{}'", hostAddress);
            okHeadersBuilder.add("X-Host-Address", hostAddress);

            String hostName = inetAddress.getHostName();
            LOGGER.debug("hostName:'{}'", hostName);
            okHeadersBuilder.add("X-Host-Name", hostName);

            String canonicalHostName = inetAddress.getCanonicalHostName();
            LOGGER.debug("canonicalHostName: '{}'", canonicalHostName);
            okHeadersBuilder.add("X-Canonical-Host-Name", canonicalHostName);

            Response.Builder builder = new Response.Builder(response);
            builder.headers(okHeadersBuilder.build());
            return builder.build();
        } else {
            return response;
        }
    }
}
