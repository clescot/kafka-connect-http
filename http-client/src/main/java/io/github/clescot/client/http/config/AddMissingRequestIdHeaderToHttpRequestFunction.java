package io.github.clescot.client.http.config;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.core.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.UnaryOperator;

public class AddMissingRequestIdHeaderToHttpRequestFunction implements UnaryOperator<HttpRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AddMissingRequestIdHeaderToHttpRequestFunction.class);
    public static final String HEADER_X_REQUEST_ID = "X-Request-ID";
    private final boolean generateMissingRequestId;
    public AddMissingRequestIdHeaderToHttpRequestFunction(boolean generateMissingRequestId) {
        this.generateMissingRequestId = generateMissingRequestId;
    }

    @Override
    public HttpRequest apply(HttpRequest httpRequest) {
        if (httpRequest == null) {
            LOGGER.warn("httpRequest is null");
            throw new IllegalArgumentException("httpRequest is null");
        }
        Map<String, List<String>> headers = httpRequest.getHeaders();
        if (headers == null) {
            headers = Maps.newHashMap();
        }

        //we generate an 'X-Request-ID' header if not present
        List<String> requestId = headers.get(HEADER_X_REQUEST_ID);
        if (requestId == null && this.generateMissingRequestId) {
            requestId = Lists.newArrayList(UUID.randomUUID().toString());
        }
        if (requestId != null) {
            headers.put(HEADER_X_REQUEST_ID, Lists.newArrayList(requestId));
            httpRequest.setHeaders(headers);
        }


        return httpRequest;
    }

    public boolean isGenerateMissingRequestId() {
        return generateMissingRequestId;
    }

    @Override
    public String toString() {
        return "{" +
                "generateMissingRequestId=" + generateMissingRequestId +
                '}';
    }
}
