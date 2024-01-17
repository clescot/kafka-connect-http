package io.github.clescot.kafka.connect.http.client.okhttp.interceptor;

import okhttp3.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

/**
 * Interceptor which add host address ('X-Host-Address'), 'host name' ('X-Host-Name') and
 * 'canonical host name' ('X-Canonical-Host-Name') of the server as Headers responses.
 */
public class InetAddressInterceptor implements Interceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(InetAddressInterceptor.class);

    @NotNull
    @Override
    public Response intercept(@NotNull Chain chain) throws IOException {

        Request request = chain.request();
        Response response = chain.proceed(request);

        Connection connection = chain.connection();
        if(connection!=null) {
            try (Socket socket = connection.socket()) {
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
            }
        }else{
            return response;
        }
    }
}
