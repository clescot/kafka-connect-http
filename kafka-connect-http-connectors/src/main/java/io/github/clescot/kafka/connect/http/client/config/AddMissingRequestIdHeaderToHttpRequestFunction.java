package io.github.clescot.kafka.connect.http.client.config;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
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
            throw new ConnectException("httpRequest is null");
        }
        Map<String, List<String>> headers = Optional.ofNullable(httpRequest.getHeaders()).orElse(Maps.newHashMap());

        //we generate an 'X-Request-ID' header if not present
        Optional<List<String>> requestId = Optional.ofNullable(httpRequest.getHeaders().get(HEADER_X_REQUEST_ID));
        if (requestId.isEmpty() && this.generateMissingRequestId) {
            requestId = Optional.of(Lists.newArrayList(UUID.randomUUID().toString()));
        }
        requestId.ifPresent(reqId -> headers.put(HEADER_X_REQUEST_ID, Lists.newArrayList(reqId)));


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
