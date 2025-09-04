package io.github.clescot.kafka.connect.http.client;

import dev.failsafe.RateLimiter;
import io.github.clescot.kafka.connect.AbstractClient;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.TrustManagerFactory;
import java.util.Map;
import java.util.Optional;

import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.*;

public abstract class AbstractHttpClient<NR,NS> extends AbstractClient<HttpExchange> implements HttpClient<NR,NS> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractHttpClient.class);

    public static final String DEFAULT_HTTP_RESPONSE_MESSAGE_STATUS_LIMIT = "1024";
    public static final String DEFAULT_HTTP_RESPONSE_HEADERS_LIMIT = "10000";
    public static final String DEFAULT_HTTP_RESPONSE_BODY_LIMIT = "100000";

    protected TrustManagerFactory trustManagerFactory;

    private Integer statusMessageLimit;
    private Integer headersLimit;
    private Integer bodyLimit;

    //rate limiter

    protected AbstractHttpClient(Map<String, Object> config) {
       super(config);

        //httpResponse
        //messageStatus limit

        int httpResponseMessageStatusLimit = Integer.parseInt(Optional.ofNullable((String)config.get(HTTP_RESPONSE_MESSAGE_STATUS_LIMIT)).orElse(DEFAULT_HTTP_RESPONSE_MESSAGE_STATUS_LIMIT));
        if(httpResponseMessageStatusLimit>0) {
            setStatusMessageLimit(httpResponseMessageStatusLimit);
        }

        int httpResponseHeadersLimit = Integer.parseInt(Optional.ofNullable((String)config.get(HTTP_RESPONSE_HEADERS_LIMIT)).orElse(DEFAULT_HTTP_RESPONSE_HEADERS_LIMIT));
        if(httpResponseHeadersLimit>0) {
            setHeadersLimit(httpResponseHeadersLimit);
        }

        //body limit
        int httpResponseBodyLimit = Integer.parseInt(Optional.ofNullable((String)config.get(HTTP_RESPONSE_BODY_LIMIT)).orElse(DEFAULT_HTTP_RESPONSE_BODY_LIMIT));
        if(httpResponseBodyLimit>0) {
            setBodyLimit(httpResponseBodyLimit);
        }

    }

    @Override
    public Integer getStatusMessageLimit() {
        return statusMessageLimit;
    }

    @Override
    public void setStatusMessageLimit(Integer statusMessageLimit) {
        this.statusMessageLimit = statusMessageLimit;
    }

    @Override
    public Integer getHeadersLimit() {
        return headersLimit;
    }


    @Override
    public void setHeadersLimit(Integer headersLimit) {
        this.headersLimit = headersLimit;
    }

    @Override
    public Integer getBodyLimit() {
        return bodyLimit;
    }

    @Override
    public void setBodyLimit(Integer bodyLimit) {
        this.bodyLimit = bodyLimit;
    }

    @Override
    public TrustManagerFactory getTrustManagerFactory() {
        return trustManagerFactory;
    }
    @Override
    public void setTrustManagerFactory(TrustManagerFactory trustManagerFactory) {
        this.trustManagerFactory = trustManagerFactory;
    }

    public abstract Object getInternalClient();

    @Override
    public String toString() {
        return this.getClass().getSimpleName()+"{" +
                "rateLimiter=" + rateLimiterToString() +
                ", trustManagerFactory=" + trustManagerFactory +
                '}';
    }

    @Override
    public HttpClient<NR, NS> customizeForUser(String vuId,HttpClient<NR, NS> genericClient){
        return genericClient;
    }

}
