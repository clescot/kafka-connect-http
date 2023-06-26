package io.github.clescot.kafka.connect.http.sink.config;

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
import java.util.function.Function;

public class AddTrackingHeadersFunction implements Function<HttpRequest,HttpRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AddTrackingHeadersFunction.class);
    public static final String HEADER_X_CORRELATION_ID = "X-Correlation-ID";
    public static final String HEADER_X_REQUEST_ID = "X-Request-ID";
    private final boolean generateMissingRequestId;
    private final boolean generateMissingCorrelationId;
    public AddTrackingHeadersFunction(boolean generateMissingRequestId,
                                      boolean generateMissingCorrelationId) {
        this.generateMissingRequestId = generateMissingRequestId;
        this.generateMissingCorrelationId = generateMissingCorrelationId;
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

        //we generate an 'X-Correlation-ID' header if not present
        Optional<List<String>> correlationId = Optional.ofNullable(httpRequest.getHeaders().get(HEADER_X_CORRELATION_ID));
        if (correlationId.isEmpty() && this.generateMissingCorrelationId) {
            correlationId = Optional.of(Lists.newArrayList(UUID.randomUUID().toString()));
        }
        correlationId.ifPresent(corrId -> headers.put(HEADER_X_CORRELATION_ID, Lists.newArrayList(corrId)));

        return httpRequest;
    }
}
