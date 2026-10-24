package io.github.clescot.client;


import io.github.clescot.core.http.Response;
@SuppressWarnings("java:S119")
public interface ResponseClient<S extends Response,NS,E> extends Client<E>{

    /**
     * convert a native response (from the implementation) to a Response.
     *
     * @param response native response
     * @return Response
     */
    S buildResponse(NS response);
}
