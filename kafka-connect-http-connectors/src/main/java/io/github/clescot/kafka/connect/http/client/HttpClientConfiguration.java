package io.github.clescot.kafka.connect.http.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import dev.failsafe.RetryPolicy;
import io.github.clescot.kafka.connect.Configuration;
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
    private final Map<String, String> settings;


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





    @Override
    public String toString() {
        return "Configuration{" +
                "id='" + id +
                "', httpClient='" + httpClient +
                '}';
    }
}
