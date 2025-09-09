package io.github.clescot.kafka.connect.http.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import dev.failsafe.RetryPolicy;
import io.github.clescot.kafka.connect.Configuration;
import io.github.clescot.kafka.connect.RequestTask;
import io.github.clescot.kafka.connect.VersionUtils;
import io.github.clescot.kafka.connect.http.client.config.HttpRequestPredicateBuilder;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.github.clescot.kafka.connect.http.client.config.HttpRequestPredicateBuilder.*;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.*;

/**
 * Configuration of the {@link HttpClient}, specific to some websites according to the configured <span class="strong">predicate</span>.
 * @param <C> client type, which is a subclass of HttpClient
 * @param <R> native HttpRequest
 * @param <S> native HttpResponse
 * <p>
 * It permits to customize :
 * <ul>
 * <li>a success http response code regex</li>
 * <li>a retry http response code regex</li>
 * <li>a custom rate limiter</li>
 * </ul>
 * Each configuration owns an Http Client instance.
 */
public class HttpClientConfiguration<C extends HttpClient<R,S>,R,S> implements Configuration<C,HttpRequest> {
    public static final VersionUtils VERSION_UTILS = new VersionUtils();
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientConfiguration.class);

    public static final String CONFIGURATION_ID = "configuration.id";


    private final Predicate<HttpRequest> predicate;







    //retry policy
    private Pattern retryResponseCodeRegex;
    private RetryPolicy<HttpExchange> retryPolicy;

    //http client
    private C httpClient;
    public final String id;
    private final Map<String, Object> settings;


    public HttpClientConfiguration(String id,
                                   Map<String,String> config,
                                   C httpClient, RetryPolicy<HttpExchange> retryPolicy) {
        this.id = id;
        Preconditions.checkNotNull(id, "id must not be null");
        Preconditions.checkNotNull(config, "httpSinkConnectorConfig must not be null");

        //configuration id prefix is not present in the resulting configMap
        this.settings = Maps.newHashMap(config);
        settings.put(CONFIGURATION_ID, id);
        //main predicate
        this.predicate = HttpRequestPredicateBuilder.build().buildPredicate(settings);
        this.httpClient = httpClient;
        this.retryPolicy = retryPolicy;

    }

    @Override
    public C getClient() {
        return httpClient;
    }

    public void setHttpClient(C httpClient) {
        this.httpClient = httpClient;
    }

    public void setRetryResponseCodeRegex(Pattern retryResponseCodeRegex) {
        this.retryResponseCodeRegex = retryResponseCodeRegex;
    }

    public void setRetryPolicy(RetryPolicy<HttpExchange> retryPolicy) {
        this.retryPolicy = retryPolicy;
    }




    public Optional<RetryPolicy<HttpExchange>> getRetryPolicy() {
        return Optional.ofNullable(retryPolicy);
    }


    public Optional<Pattern> getRetryResponseCodeRegex() {
        return Optional.ofNullable(retryResponseCodeRegex);
    }



    public boolean matches(HttpRequest httpRequest) {
        return this.predicate.test(httpRequest);
    }


    public String getId() {
        return id;
    }



    private String predicateToString() {
        StringBuilder result = new StringBuilder("{");
        String urlRegex = (String) settings.get(URL_REGEX);
        if(urlRegex!=null) {
            result.append("urlRegex:'").append(urlRegex).append("'");
        }
        String methodRegex = (String) settings.get(METHOD_REGEX);
        if(methodRegex!=null) {
            result.append(",methodRegex:").append(methodRegex).append("'");
        }
        String bodytypeRegex = (String) settings.get(BODYTYPE_REGEX);
        if(bodytypeRegex!=null) {
            result.append(",bodytypeRegex:").append(bodytypeRegex).append("'");
        }
        String headerKeyRegex = (String) settings.get(HEADER_KEY_REGEX);
        if(headerKeyRegex!=null) {
            result.append(",headerKeyRegex:").append(headerKeyRegex).append("'");
        }
        result.append("}");
        return result.toString();
    }

    private String retryPolicyToString(){
        StringBuilder result = new StringBuilder("{");
        if(retryResponseCodeRegex!=null){
            result.append("retryResponseCodeRegex:'").append(retryResponseCodeRegex).append("'");
        }
        String retries = (String) settings.get(RETRIES);
        if(retries!=null){
            result.append(", retries:'").append(retries).append("'");
        }
        String retryDelayInMs = (String) settings.get(RETRY_DELAY_IN_MS);
        if(retryDelayInMs!=null){
            result.append(", retryDelayInMs:'").append(retryDelayInMs).append("'");
        }
        String maxRetryDelayInMs = (String) settings.get(RETRY_MAX_DELAY_IN_MS);
        if(maxRetryDelayInMs!=null){
            result.append(", maxRetryDelayInMs:'").append(maxRetryDelayInMs).append("'");
        }
        String retryDelayFactor = (String) settings.get(RETRY_DELAY_FACTOR);
        if(retryDelayFactor!=null){
            result.append(", retryDelayFactor:'").append(retryDelayFactor).append("'");
        }
        String retryjitterInMs = (String) settings.get(RETRY_JITTER_IN_MS);
        if(retryjitterInMs!=null){
            result.append(", retryjitterInMs:'").append(retryjitterInMs).append("'");
        }
        result.append("}");
        return result.toString();
    }
    @Override
    public String toString() {
        return "Configuration{" +
                "id='" + id +
                "', predicate='" + predicateToString() +
                "', retryPolicy='" + retryPolicyToString() +
                "', httpClient='" + httpClient +
                '}';
    }
}
