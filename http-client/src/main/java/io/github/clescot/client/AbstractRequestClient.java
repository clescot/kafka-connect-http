package io.github.clescot.client;

import dev.failsafe.RateLimiter;
import io.github.clescot.core.http.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.github.clescot.client.Constants.*;
import static io.github.clescot.client.http.HttpClientFactory.CONFIGURATION_ID;

public abstract class AbstractRequestClient<R extends Request,NR,E> implements RequestClient<R,NR,E> {
    public static final String RATE_LIMITER_PERMITS_PER_EXECUTION = "rate.limiter.permits.per.execution";
    public static final String DEFAULT_RATE_LIMITER_ONE_PERMIT_PER_CALL = "one";
    public static final Logger LOGGER = LoggerFactory.getLogger(AbstractRequestClient.class);
    private Optional<RateLimiter<E>> rateLimiter = Optional.empty();
    protected Map<String, String> config;
    protected String configurationId;

    public AbstractRequestClient(Map<String, String> config) {
        this.config = config;
        configurationId = Optional.ofNullable(config.get(CONFIGURATION_ID)).orElse(Configuration.DEFAULT_CONFIGURATION_ID);
        setRateLimiter(buildRateLimiter(config, configurationId));
    }

    public void setRateLimiter(RateLimiter<E> rateLimiter) {
        this.rateLimiter = Optional.ofNullable(rateLimiter);
    }

    @Override
    public Optional<RateLimiter<E>> getRateLimiter() {
        return this.rateLimiter;
    }

    public Map<String, String> getConfig() {
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

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        AbstractRequestClient<?,?,?> that = (AbstractRequestClient<?,?,?>) o;
        return getConfig().equals(that.getConfig());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getConfig());
    }

}
