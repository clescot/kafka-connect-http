package io.github.clescot.kafka.connect;


public interface ResponseClient<S,NS> extends Client{

    /**
     * convert a native response (from the implementation) to a Response.
     *
     * @param response native response
     * @return Response
     */
    S buildResponse(NS response);
}
