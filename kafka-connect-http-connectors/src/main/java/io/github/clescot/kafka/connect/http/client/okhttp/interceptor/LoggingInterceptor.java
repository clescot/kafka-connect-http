package io.github.clescot.kafka.connect.http.client.okhttp.interceptor;

import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Interceptor which logs requests and responses, and the timing of the exchange.
 */
public class LoggingInterceptor implements Interceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingInterceptor.class);

        @NotNull
        @Override public Response intercept(Interceptor.Chain chain) throws IOException {
            Request request = chain.request();

            long t1 = System.nanoTime();
            if(LOGGER.isTraceEnabled()) {
                LOGGER.trace(String.format("Sending request %s on %s%n%s",
                        request.url(), chain.connection(), request.headers()));
            }
            Response response = chain.proceed(request);

            long t2 = System.nanoTime();
            if(LOGGER.isDebugEnabled()) {
                //elapsed time : local code execution + network time + remote server-side execution time
                //does not contains the waiting time from the rateLimiter
                //the rate limiting mechanism is present before this execution
                //so the code has already wait if needed
                double elapsedTime = (t2 - t1) / 1e6d;
                LOGGER.debug(String.format("Received response for %s on %s%n%s in %.1fms%n%s %s%n%s",
                        response.request().url(), chain.connection(), request.headers(),elapsedTime, response.code(), response.message(), response.headers()));
            }
            return response;
        }
}
