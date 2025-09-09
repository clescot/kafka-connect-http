package io.github.clescot.kafka.connect;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import dev.failsafe.RateLimiter;
import dev.failsafe.RateLimiterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;

import static io.github.clescot.kafka.connect.http.client.HttpClientConfiguration.CONFIGURATION_ID;
import static io.github.clescot.kafka.connect.http.client.HttpClientConfiguration.STATIC_SCOPE;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.*;

public abstract class  AbstractClient<E> implements Client<E> {
    Logger LOGGER = LoggerFactory.getLogger(AbstractClient.class);
    public static final Map<String, RateLimiter> SHARED_RATE_LIMITERS = Maps.newHashMap();
    private Optional<RateLimiter<E>> rateLimiter = Optional.empty();
    protected Map<String, String> config;
    protected String configurationId;

    public AbstractClient(Map<String, String> config) {
        this.config = config;
        configurationId = Optional.ofNullable(config.get(CONFIGURATION_ID)).orElse(Configuration.DEFAULT_CONFIGURATION_ID);
        setRateLimiter(buildRateLimiter(config, configurationId));
    }

    public RateLimiter<E> buildRateLimiter(Map<String, String> configMap, String configurationId) {
        RateLimiter<E> myRateLimiter = null;
        if (configMap.containsKey(RATE_LIMITER_MAX_EXECUTIONS)) {
            long maxExecutions = Long.parseLong(configMap.get(RATE_LIMITER_MAX_EXECUTIONS));
            LOGGER.trace("configuration '{}' : maxExecutions :{}", configurationId, maxExecutions);
            long periodInMs = Long.parseLong(Optional.ofNullable(configMap.get(RATE_LIMITER_PERIOD_IN_MS)).orElse(1000 + ""));
            LOGGER.trace("configuration '{}' : periodInMs :{}", configurationId, periodInMs);
            if (configMap.containsKey(RATE_LIMITER_SCOPE) && STATIC_SCOPE.equalsIgnoreCase(configMap.get(RATE_LIMITER_SCOPE))) {
                LOGGER.trace("configuration '{}' : rateLimiter scope is 'static'", configurationId);
                Optional<RateLimiter<E>> sharedRateLimiter = Optional.ofNullable(SHARED_RATE_LIMITERS.get(configurationId));
                if (sharedRateLimiter.isPresent()) {
                    myRateLimiter = sharedRateLimiter.get();
                    LOGGER.trace("configuration '{}' : rate limiter is already shared", configurationId);
                } else {
                    myRateLimiter = RateLimiter.<E>smoothBuilder(maxExecutions, Duration.of(periodInMs, ChronoUnit.MILLIS)).build();
                    registerRateLimiter(configurationId, myRateLimiter);
                }
            } else {
                myRateLimiter = RateLimiter.<E>smoothBuilder(maxExecutions, Duration.of(periodInMs, ChronoUnit.MILLIS)).build();
            }
            RateLimiterConfig<E> rateLimiterConfig = myRateLimiter.getConfig();
            LOGGER.trace("configuration '{}' rate limiter configured : 'maxRate:{},maxPermits{},period:{},maxWaitTime:{}'",
                    configurationId,
                    rateLimiterConfig.getMaxRate(),
                    rateLimiterConfig.getMaxPermits(),
                    rateLimiterConfig.getPeriod(),
                    rateLimiterConfig.getMaxWaitTime()
            );
        } else {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("configuration '{}' : rate limiter is not configured", configurationId);
                LOGGER.trace(Joiner.on(",\n").withKeyValueSeparator("=").join(configMap.entrySet()));
            }
        }
        return myRateLimiter;
    }

    public void registerRateLimiter(String configurationId, RateLimiter<E> rateLimiter) {
        Preconditions.checkNotNull(configurationId, "we cannot register a rateLimiter for a 'null' configurationId");
        Preconditions.checkNotNull(rateLimiter, "we cannot register a 'null' rate limiter for the configurationId " + configurationId);
        LOGGER.info("registration of a shared rateLimiter for the configurationId '{}'", configurationId);
        SHARED_RATE_LIMITERS.put(configurationId, rateLimiter);
    }


    public void setRateLimiter(RateLimiter<E> rateLimiter) {
        this.rateLimiter = Optional.ofNullable(rateLimiter);
    }

    @Override
    public Optional<RateLimiter<E>> getRateLimiter() {
        return this.rateLimiter;
    }

    protected Map<String, String> getConfig() {
        return config;
    }

    @Override
    public String getPermitsPerExecution(){
        return config.getOrDefault(RATE_LIMITER_PERMITS_PER_EXECUTION, DEFAULT_RATE_LIMITER_ONE_PERMIT_PER_CALL);
    }


    public String rateLimiterToString(){
        StringBuilder result = new StringBuilder("{");

        String rateLimiterMaxExecutions = config.get(RATE_LIMITER_MAX_EXECUTIONS);
        if(rateLimiterMaxExecutions!=null){
            result.append("rateLimiterMaxExecutions:'").append(rateLimiterMaxExecutions).append("'");
        }
        String rateLimiterPeriodInMs = config.get(RATE_LIMITER_PERIOD_IN_MS);
        if(rateLimiterPeriodInMs!=null){
            result.append(",rateLimiterPeriodInMs:'").append(rateLimiterPeriodInMs).append("'");
        }
        String rateLimiterScope = config.get(RATE_LIMITER_SCOPE);
        if(rateLimiterScope!=null){
            result.append(",rateLimiterScope:'").append(rateLimiterScope).append("'");
        }
        result.append("}");
        return result.toString();
    }


}
