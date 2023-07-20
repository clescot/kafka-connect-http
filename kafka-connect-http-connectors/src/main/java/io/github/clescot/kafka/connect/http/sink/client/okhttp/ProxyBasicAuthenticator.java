package io.github.clescot.kafka.connect.http.sink.client.okhttp;

import okhttp3.*;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.Charset;

public class ProxyBasicAuthenticator implements Authenticator {


    private final String username;
    private final String password;
    private final Charset charset;

    public ProxyBasicAuthenticator(String username, String password, Charset charset) {
        this.username = username;
        this.password = password;
        this.charset = charset;
    }

    @Nullable
    @Override public Request authenticate(Route route, Response response) {
        String credential = Credentials.basic(username, password,charset);
        return response.request().newBuilder()
                .header("Proxy-Authorization", credential)
                .build();
    }
}
