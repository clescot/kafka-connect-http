package io.github.clescot.kafka.connect;

import io.github.clescot.kafka.connect.http.core.Request;

/**
 *  A client interface that handles requests and native requests.
 * @param <R> request
 * @param <NR> native request
 */
@SuppressWarnings("java:S119")
public interface RequestClient<R extends Request,NR,E> extends Client<E>{
    /**
     * convert an Request into a native request.
     *
     * @param request to build.
     * @return native request.
     */
    NR buildNativeRequest(R request);


    R buildRequest(NR nativeRequest);

}
