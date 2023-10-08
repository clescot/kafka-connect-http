package io.github.clescot.kafka.connect.http.client.okhttp.interceptor;

import com.google.common.base.Preconditions;
import okhttp3.Headers;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class UserAgentInterceptor implements Interceptor {

    public static final String USER_AGENT = "User-Agent";
    private final String userAgent;
    public UserAgentInterceptor(String userAgent) {
        Preconditions.checkNotNull(userAgent);
        this.userAgent = userAgent;
    }

    @NotNull
    @Override
    public Response intercept(@NotNull Chain chain) throws IOException {
        Request request = chain.request();

        Headers originalHeaders = request.headers();
        Headers.Builder builder = new Headers.Builder();
        builder.addAll(originalHeaders);
        builder.removeAll(USER_AGENT);
        builder.add(USER_AGENT,userAgent);
        Headers modifiedHeaders = builder.build();

        Request modifiedRequest = request.newBuilder().headers(modifiedHeaders).build();

        return chain.proceed(modifiedRequest);
    }
}
