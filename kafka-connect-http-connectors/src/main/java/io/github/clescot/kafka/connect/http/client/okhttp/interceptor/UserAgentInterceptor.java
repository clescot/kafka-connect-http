package io.github.clescot.kafka.connect.http.client.okhttp.interceptor;

import com.google.common.base.Preconditions;
import okhttp3.Headers;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;

public class UserAgentInterceptor implements Interceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserAgentInterceptor.class);
    public static final String USER_AGENT = "User-Agent";
    private final List<String> userAgents;
    private final Random random;

    public UserAgentInterceptor(List<String> userAgents, Random random) {
        Preconditions.checkNotNull(userAgents);
        this.random = random;
        this.userAgents = userAgents;
    }

    private String getUserAgents(){
        return userAgents.get(random.nextInt(userAgents.size()));
    }

    @NotNull
    @Override
    public Response intercept(@NotNull Chain chain) throws IOException {
        Request request = chain.request();

        Headers originalHeaders = request.headers();
        Headers.Builder builder = new Headers.Builder();
        builder.addAll(originalHeaders);
        builder.removeAll(USER_AGENT);
        String userAgent = getUserAgents();
        LOGGER.debug("User-Agent:{}",userAgent);
        builder.add(USER_AGENT, userAgent);
        Headers modifiedHeaders = builder.build();

        Request modifiedRequest = request.newBuilder().headers(modifiedHeaders).build();

        Response response = chain.proceed(modifiedRequest);
        List<String> headers = response.request().headers("User-Agent");
        if(headers!=null&& !headers.isEmpty()) {
            LOGGER.debug("User-Agent after response:{}", headers.get(0));
        }
        return response;
    }
}
