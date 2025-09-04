package io.github.clescot.kafka.connect;

import com.google.common.base.Stopwatch;
import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.core.Response;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("java:S119")
public interface RequestResponseClient<R extends Request, NR, S extends Response, NS, E extends Exchange> extends RequestClient<R, NR,E>, ResponseClient<S, NS,E> {


    E buildExchange(R httpRequest,
                    S httpResponse,
                    Stopwatch stopwatch,
                    OffsetDateTime now,
                    AtomicInteger attempts,
                    boolean success,
                    Map<String,String> attributes);

    /**
     * raw native HttpRequest call.
     * @param request native HttpRequest
     * @return CompletableFuture of a native HttpResponse.
     */
    CompletableFuture<NS> nativeCall(NR request);
}
