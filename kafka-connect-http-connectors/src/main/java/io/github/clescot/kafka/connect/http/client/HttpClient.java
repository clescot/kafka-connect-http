package io.github.clescot.kafka.connect.http.client;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.RequestResponseClient;
import io.github.clescot.kafka.connect.http.client.config.AddSuccessStatusToHttpExchangeFunction;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.TrustManagerFactory;
import java.net.CookiePolicy;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * execute the HTTP call.
 *
 * @param <NR> native HttpRequest
 * @param <NS> native HttpResponse
 */
@SuppressWarnings({"java:S119"})
public interface HttpClient<NR, NS> extends RequestResponseClient<HttpRequest, NR, HttpResponse, NS, HttpExchange>, Cloneable {
    int SERVER_ERROR_STATUS_CODE = 500;


    Logger LOGGER = LoggerFactory.getLogger(HttpClient.class);


    default HttpExchange buildExchange(HttpRequest request,
                                       HttpResponse response,
                                       Stopwatch stopwatch,
                                       OffsetDateTime now,
                                       AtomicInteger attempts,
                                       Map<String, Object> attributes,
                                       Map<String, Long> timings) {
        Preconditions.checkNotNull(request, "'httpRequest' is null");
        return HttpExchange.Builder.anHttpExchange()
                //request
                .withHttpRequest(request)
                //response
                .withHttpResponse(response)
                //technical metadata
                //time elapsed during http call
                .withDuration(stopwatch!=null?stopwatch.elapsed(TimeUnit.MILLISECONDS):0)
                //at which moment occurs the beginning of the http call
                .at(now)
                .withAttempts(attempts)
                .withAttributes(attributes != null && !attributes.isEmpty() ? attributes : Maps.newHashMap())
                .withTimings(timings)
                .build();
    }

    HttpClient<NR, NS> customizeForUser(String vuId);

    CookiePolicy getCookiePolicy();

    Function<HttpRequest, HttpRequest> getEnrichRequestFunction();

    Integer getStatusMessageLimit();

    void setStatusMessageLimit(Integer statusMessageLimit);

    Integer getHeadersLimit();

    void setHeadersLimit(Integer headersLimit);

    Integer getBodyLimit();

    void setBodyLimit(Integer bodyLimit);

    TrustManagerFactory getTrustManagerFactory();

    void setTrustManagerFactory(TrustManagerFactory trustManagerFactory);

    void setAddSuccessStatusToHttpExchangeFunction(Pattern addSuccessStatusToHttpExchangeFunction);

    AddSuccessStatusToHttpExchangeFunction getAddSuccessStatusToHttpExchangeFunction();

    default HttpExchange getErrorResponse(HttpRequest request, AtomicInteger attempts, Throwable throwable, Stopwatch rateLimitedStopWatch, OffsetDateTime now){
        HttpResponse httpResponse = new HttpResponse(400, throwable.getMessage());
        Map<String, List<String>> responseHeaders = Maps.newHashMap();
        responseHeaders.put(THROWABLE_CLASS, Lists.newArrayList(throwable.getCause().getClass().getName()));
        responseHeaders.put(THROWABLE_MESSAGE, Lists.newArrayList(throwable.getCause().getMessage()));
        httpResponse.setHeaders(responseHeaders);
        if(LOGGER.isErrorEnabled()) {
            LOGGER.error("exception thrown :'{}'", throwable.toString());
        }
        return buildExchange(request, httpResponse, rateLimitedStopWatch, now, attempts,
                Maps.newHashMap(),
                Maps.newHashMap());
    }

}
