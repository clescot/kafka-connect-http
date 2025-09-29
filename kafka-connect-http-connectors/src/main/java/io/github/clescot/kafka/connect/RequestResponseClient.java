package io.github.clescot.kafka.connect;

import com.google.common.base.Stopwatch;
import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.core.Response;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A client that can handle both request and response.
 * @param <R> request type
 * @param <NR> native request type
 * @param <S> response type
 * @param <NS> native response type
 * @param <E> exchange type
 */
@SuppressWarnings("java:S119")
public interface RequestResponseClient<R extends Request, NR, S extends Response, NS, E extends Exchange> extends RequestClient<R, NR,E>, ResponseClient<S, NS,E> {


    E buildExchange(R request,
                    S response,
                    Stopwatch stopwatch,
                    OffsetDateTime now,
                    AtomicInteger attempts,
                    boolean success,
                    Map<String,Object> attributes,
                    Map<String,Long> timings);

    /**
     * raw native Request call.
     * @param request native Request
     * @return CompletableFuture of a native Response.
     */
    CompletableFuture<NS> nativeCall(NR request);

    Map<String, Long> getTimings(NR request, CompletableFuture<NS> response);
}
