package io.github.clescot.kafka.connect.http.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.AbstractClient;
import io.github.clescot.kafka.connect.http.client.config.*;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.TrustManagerFactory;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;

import static io.github.clescot.kafka.connect.http.core.VersionUtils.VERSION;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.*;

public abstract class AbstractHttpClient<NR,NS> extends AbstractClient<HttpExchange> implements HttpClient<NR,NS> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractHttpClient.class);

    public static final String DEFAULT_HTTP_RESPONSE_MESSAGE_STATUS_LIMIT = "1024";
    public static final String DEFAULT_HTTP_RESPONSE_HEADERS_LIMIT = "10000";
    public static final String DEFAULT_HTTP_RESPONSE_BODY_LIMIT = "100000";
    protected final Random random;
    protected AddSuccessStatusToHttpExchangeFunction addSuccessStatusToHttpExchangeFunction;

    protected TrustManagerFactory trustManagerFactory;

    private Integer statusMessageLimit;
    private Integer headersLimit;
    private Integer bodyLimit;
    public static final String USER_AGENT_HTTP_CLIENT_DEFAULT_MODE = "http_client";
    public static final String USER_AGENT_PROJECT_MODE = "project";
    public static final String USER_AGENT_CUSTOM_MODE = "custom";
    private final Function<HttpRequest, HttpRequest> enrichRequestFunction;
    private final boolean cookieEnabled;
    //rate limiter

    protected AbstractHttpClient(Map<String, String> config,Random random) {
       super(config);

        //httpResponse
        //messageStatus limit

        int httpResponseMessageStatusLimit = Integer.parseInt(Optional.ofNullable(config.get(HTTP_RESPONSE_MESSAGE_STATUS_LIMIT)).orElse(DEFAULT_HTTP_RESPONSE_MESSAGE_STATUS_LIMIT));
        if(httpResponseMessageStatusLimit>0) {
            setStatusMessageLimit(httpResponseMessageStatusLimit);
        }

        int httpResponseHeadersLimit = Integer.parseInt(Optional.ofNullable(config.get(HTTP_RESPONSE_HEADERS_LIMIT)).orElse(DEFAULT_HTTP_RESPONSE_HEADERS_LIMIT));
        if(httpResponseHeadersLimit>0) {
            setHeadersLimit(httpResponseHeadersLimit);
        }

        //body limit
        int httpResponseBodyLimit = Integer.parseInt(Optional.ofNullable(config.get(HTTP_RESPONSE_BODY_LIMIT)).orElse(DEFAULT_HTTP_RESPONSE_BODY_LIMIT));
        if(httpResponseBodyLimit>0) {
            setBodyLimit(httpResponseBodyLimit);
        }
        this.enrichRequestFunction = buildEnrichRequestFunction(config,random);
        this.random = random;

        this.cookieEnabled = Boolean.parseBoolean(Optional.ofNullable(config.get("")).orElse("true"));
    }

    @Override
    public boolean isCookieEnabled() {
        return cookieEnabled;
    }

    @Override
    public Function<HttpRequest, HttpRequest> getEnrichRequestFunction() {
        return enrichRequestFunction;
    }

    private Function<HttpRequest,HttpRequest> buildEnrichRequestFunction(Map<String,String> settings, Random random) {

        //enrich request
        List<Function<HttpRequest,HttpRequest>> enrichRequestFunctions = Lists.newArrayList();
        //build addStaticHeadersFunction
        Optional<String> staticHeaderParam = Optional.ofNullable(settings.get(STATIC_REQUEST_HEADER_NAMES));
        Map<String, List<String>> staticRequestHeaders = Maps.newHashMap();
        if (staticHeaderParam.isPresent()) {
            String[] staticRequestHeaderNames = staticHeaderParam.get().split(",");
            for (String headerName : staticRequestHeaderNames) {
                String value = settings.get(STATIC_REQUEST_HEADER_PREFIX + headerName);
                Preconditions.checkNotNull(value, "'" + headerName + "' is not configured as a parameter.");
                ArrayList<String> values = Lists.newArrayList(value);
                staticRequestHeaders.put(headerName, values);
                LOGGER.debug("static header {}:{}", headerName, values);
            }
        }
        enrichRequestFunctions.add(new AddStaticHeadersToHttpRequestFunction(staticRequestHeaders));

        //AddMissingRequestIdHeaderToHttpRequestFunction
        boolean generateMissingRequestId = Boolean.parseBoolean(settings.get(GENERATE_MISSING_REQUEST_ID));
        enrichRequestFunctions.add( new AddMissingRequestIdHeaderToHttpRequestFunction(generateMissingRequestId));

        //AddMissingCorrelationIdHeaderToHttpRequestFunction
        boolean generateMissingCorrelationId = Boolean.parseBoolean(settings.get(GENERATE_MISSING_CORRELATION_ID));
        enrichRequestFunctions.add(new AddMissingCorrelationIdHeaderToHttpRequestFunction(generateMissingCorrelationId));

        //activateUserAgentHeaderToHttpRequestFunction
        String activateUserAgentHeaderToHttpRequestFunction = settings.getOrDefault(USER_AGENT_OVERRIDE, USER_AGENT_HTTP_CLIENT_DEFAULT_MODE);
        if (USER_AGENT_HTTP_CLIENT_DEFAULT_MODE.equalsIgnoreCase(activateUserAgentHeaderToHttpRequestFunction)) {
            LOGGER.trace("userAgentHeaderToHttpRequestFunction : 'http_client' configured. No need to activate UserAgentInterceptor");
        }else if(USER_AGENT_PROJECT_MODE.equalsIgnoreCase(activateUserAgentHeaderToHttpRequestFunction)){
            String projectUserAgent = "Mozilla/5.0 (compatible;kafka-connect-http/"+ VERSION +"; "+this.getEngineId()+"; https://github.com/clescot/kafka-connect-http)";
            enrichRequestFunctions.add(new AddUserAgentHeaderToHttpRequestFunction(Lists.newArrayList(projectUserAgent), random));
        }else if(USER_AGENT_CUSTOM_MODE.equalsIgnoreCase(activateUserAgentHeaderToHttpRequestFunction)){
            String userAgentValuesAsString = settings.getOrDefault(USER_AGENT_CUSTOM_VALUES, StringUtils.EMPTY).toString();
            List<String> userAgentValues = Arrays.asList(userAgentValuesAsString.split("\\|"));
            enrichRequestFunctions.add(new AddUserAgentHeaderToHttpRequestFunction(userAgentValues, random));
        }else{
            LOGGER.trace("user agent interceptor : '{}' configured. No need to activate UserAgentInterceptor",activateUserAgentHeaderToHttpRequestFunction);
        }

        return enrichRequestFunctions.stream().reduce(Function.identity(), Function::andThen);
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
    public HttpClient<NR, NS> customizeForUser(String vuId){
        return this;
    }

    @Override
    public void setAddSuccessStatusToHttpExchangeFunction(Pattern pattern) {
        this.addSuccessStatusToHttpExchangeFunction = new AddSuccessStatusToHttpExchangeFunction(pattern);
    }

    @Override
    public AddSuccessStatusToHttpExchangeFunction getAddSuccessStatusToHttpExchangeFunction() {
        return addSuccessStatusToHttpExchangeFunction;
    }
}
