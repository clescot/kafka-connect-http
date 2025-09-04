package io.github.clescot.kafka.connect;

import io.github.clescot.kafka.connect.http.core.HttpResponse;

public interface ResponseClient<S,NS> extends Client{

    /**
     * convert a native response (from the implementation) to a Response.
     *
     * @param response native response
     * @return Response
     */
    S buildResponse(NS response);
}
