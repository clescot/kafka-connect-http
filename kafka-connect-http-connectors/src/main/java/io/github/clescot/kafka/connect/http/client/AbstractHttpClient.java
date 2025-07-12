package io.github.clescot.kafka.connect.http.client;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import dev.failsafe.RateLimiter;
import dev.failsafe.RateLimiterConfig;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.TrustManagerFactory;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;

import static io.github.clescot.kafka.connect.http.client.Configuration.CONFIGURATION_ID;
import static io.github.clescot.kafka.connect.http.client.Configuration.STATIC_SCOPE;
import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

public abstract class AbstractHttpClient<R,S> implements HttpClient<R,S> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractHttpClient.class);

    public static final String DEFAULT_HTTP_RESPONSE_MESSAGE_STATUS_LIMIT = "1024";
    public static final String DEFAULT_HTTP_RESPONSE_HEADERS_LIMIT = "10000";
    public static final String DEFAULT_HTTP_RESPONSE_BODY_LIMIT = "100000";
    protected Map<String, Object> config;
    private Optional<RateLimiter<HttpExchange>> rateLimiter = Optional.empty();
    protected TrustManagerFactory trustManagerFactory;
    protected String configurationId;
    private Integer statusMessageLimit;
    private Integer headersLimit;
    private Integer bodyLimit;

    //rate limiter
    private static final Map<String, RateLimiter<HttpExchange>> sharedRateLimiters = Maps.newHashMap();

    protected AbstractHttpClient(Map<String, Object> config) {
        this.config = config;
        configurationId = (String) config.get(CONFIGURATION_ID);
        Preconditions.checkNotNull(configurationId,"configuration must have an id");
        setRateLimiter(buildRateLimiter(config));

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
    public String getPermitsPerExecution(){
        return (String) config.getOrDefault(RATE_LIMITER_PERMITS_PER_EXECUTION, DEFAULT_RATE_LIMITER_ONE_PERMIT_PER_CALL);
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
    public void setRateLimiter(RateLimiter<HttpExchange> rateLimiter) {
        this.rateLimiter = Optional.ofNullable(rateLimiter);
    }

    @Override
    public Optional<RateLimiter<HttpExchange>> getRateLimiter() {
        return this.rateLimiter;
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

    private String rateLimiterToString(){
        StringBuilder result = new StringBuilder("{");

        String rateLimiterMaxExecutions = (String) config.get(RATE_LIMITER_MAX_EXECUTIONS);
        if(rateLimiterMaxExecutions!=null){
            result.append("rateLimiterMaxExecutions:'").append(rateLimiterMaxExecutions).append("'");
        }
        String rateLimiterPeriodInMs = (String) config.get(RATE_LIMITER_PERIOD_IN_MS);
        if(rateLimiterPeriodInMs!=null){
            result.append(",rateLimiterPeriodInMs:'").append(rateLimiterPeriodInMs).append("'");
        }
        String rateLimiterScope = (String) config.get(RATE_LIMITER_SCOPE);
        if(rateLimiterScope!=null){
            result.append(",rateLimiterScope:'").append(rateLimiterScope).append("'");
        }
        result.append("}");
        return result.toString();
    }

    private RateLimiter<HttpExchange> buildRateLimiter(Map<String, Object> configMap) {
        RateLimiter<HttpExchange> myRateLimiter = null;
        if (configMap.containsKey(RATE_LIMITER_MAX_EXECUTIONS)) {
            long maxExecutions = Long.parseLong((String) configMap.get(RATE_LIMITER_MAX_EXECUTIONS));
            LOGGER.trace("configuration '{}' : maxExecutions :{}",configurationId,maxExecutions);
            long periodInMs = Long.parseLong(Optional.ofNullable((String) configMap.get(RATE_LIMITER_PERIOD_IN_MS)).orElse(1000 + ""));
            LOGGER.trace("configuration '{}' : periodInMs :{}",configurationId,periodInMs);
            if (configMap.containsKey(RATE_LIMITER_SCOPE) && STATIC_SCOPE.equalsIgnoreCase((String) configMap.get(RATE_LIMITER_SCOPE))) {
                LOGGER.trace("configuration '{}' : rateLimiter scope is 'static'",configurationId);
                Optional<RateLimiter<HttpExchange>> sharedRateLimiter = Optional.ofNullable(sharedRateLimiters.get(configurationId));
                if (sharedRateLimiter.isPresent()) {
                    myRateLimiter = sharedRateLimiter.get();
                    LOGGER.trace("configuration '{}' : rate limiter is already shared",configurationId);
                } else {
                    myRateLimiter = RateLimiter.<HttpExchange>smoothBuilder(maxExecutions, Duration.of(periodInMs, ChronoUnit.MILLIS)).build();
                    registerRateLimiter(configurationId, myRateLimiter);
                }
            } else {
                myRateLimiter = RateLimiter.<HttpExchange>smoothBuilder(maxExecutions, Duration.of(periodInMs, ChronoUnit.MILLIS)).build();
            }
            RateLimiterConfig<HttpExchange> rateLimiterConfig = myRateLimiter.getConfig();
            LOGGER.trace("configuration '{}' rate limiter configured : 'maxRate:{},maxPermits{},period:{},maxWaitTime:{}'",configurationId,rateLimiterConfig.getMaxRate(),rateLimiterConfig.getMaxPermits(),rateLimiterConfig.getPeriod(),rateLimiterConfig.getMaxWaitTime());
        }else{
            if(LOGGER.isTraceEnabled()) {
                LOGGER.trace("configuration '{}' : rate limiter is not configured", configurationId);
                LOGGER.trace(Joiner.on(",\n").withKeyValueSeparator("=").join(configMap.entrySet()));
            }
        }
        return myRateLimiter;
    }


    public static void registerRateLimiter(String configurationId, RateLimiter<HttpExchange> rateLimiter) {
        Preconditions.checkNotNull(configurationId, "we cannot register a rateLimiter for a 'null' configurationId");
        Preconditions.checkNotNull(rateLimiter, "we cannot register a 'null' rate limiter for the configurationId " + configurationId);
        LOGGER.info("registration of a shared rateLimiter for the configurationId '{}'", configurationId);
        sharedRateLimiters.put(configurationId, rateLimiter);
    }
}
