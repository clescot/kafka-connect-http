package com.github.clescot.kafka.connect.http.sink.client;

import com.github.clescot.kafka.connect.http.HttpExchange;
import com.github.clescot.kafka.connect.http.HttpRequest;
import com.github.clescot.kafka.connect.http.HttpResponse;
import com.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dev.failsafe.Failsafe;
import dev.failsafe.RateLimiter;
import dev.failsafe.RetryPolicy;
import org.asynchttpclient.*;
import org.asynchttpclient.proxy.ProxyServer;
import org.asynchttpclient.proxy.ProxyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class HttpClient {

    private final static Logger LOGGER = LoggerFactory.getLogger(HttpClient.class);
    public static final int SERVER_ERROR_STATUS_CODE = 500;
    public static final String PROXY_PREFIX = "proxy-";
    public static final String REALM_PREFIX = "realm-";
    private static final String WS_PROXY_HOST = "host";
    private static final String WS_PROXY_PORT = "port";
    private static final String WS_PROXY_SECURED_PORT = "secured-port";
    private static final String WS_PROXY_NON_PROXY_HOSTS = "non-proxy-hosts";
    private static final String WS_PROXY_TYPE = "type";
    private static final String WS_REALM_PRINCIPAL_NAME = "principal-name";
    private static final String WS_REALM_ALGORITHM = "algorithm";
    private static final String WS_REALM_CHARSET = "charset";
    private static final String WS_REALM_LOGIN_CONTEXT_NAME = "login-context-name";
    private static final String WS_REALM_METHOD = "method";
    private static final String WS_REALM_NC = "nc";
    private static final String WS_REALM_NONCE = "nonce";
    private static final String WS_REALM_NTLM_DOMAIN = "ntlm-domain";
    private static final String WS_REALM_NTLM_HOST = "ntlm-host";
    private static final String WS_REALM_OMIT_QUERY = "omit-query";
    private static final String WS_REALM_OPAQUE = "opaque";
    private static final String WS_REALM_QOP = "qop";
    private static final String WS_REALM_SERVICE_PRINCIPAL_NAME = "service-principal-name";
    private static final String WS_REALM_NAME = "name";
    private static final String WS_REALM_USE_PREEMPTIVE_AUTH = "use-preemptive-auth";
    private static final String WS_REALM_AUTH_SCHEME = "auth-scheme";
    public static final String HTTP_PROXY_TYPE = "HTTP";
    public static final String UTF_8 = "UTF-8";
    public static final String UTC_ZONE_ID = "UTC";
    public static final boolean FAILURE = false;
    public static final boolean SUCCESS = true;
    public static final int ONE_HTTP_REQUEST = 1;


    private AsyncHttpClient asyncHttpClient;
    private Optional<RetryPolicy<HttpExchange>> defaultRetryPolicy = Optional.empty();
    private RateLimiter<HttpExchange> defaultRateLimiter = RateLimiter.<HttpExchange>smoothBuilder(HttpSinkConfigDefinition.DEFAULT_RATE_LIMITER_MAX_EXECUTIONS_VALUE, Duration.of(HttpSinkConfigDefinition.DEFAULT_RATE_LIMITER_PERIOD_IN_MS_VALUE, ChronoUnit.MILLIS)).build();;
    public static final String BLANK_RESPONSE_CONTENT = "";
    public static final String WS_REQUEST_TIMEOUT_IN_MS = "request-timeout-in-ms";
    public static final String WS_READ_TIMEOUT_IN_MS = "read-timeout-in-ms";
    private static final String WS_REALM_PASS = "password";

    private final Map<String, Pattern> httpSuccessCodesPatterns = Maps.newHashMap();
    private final HttpClientAsyncCompletionHandler asyncCompletionHandler = new HttpClientAsyncCompletionHandler();

    public HttpClient(AsyncHttpClient asyncHttpClient) {
        this.asyncHttpClient = asyncHttpClient;
    }

    /**
     * @param httpRequest
     * @return
     */
    public HttpExchange call(HttpRequest httpRequest) {
        HttpExchange httpExchange = null;

        if (httpRequest != null) {


            Optional<RetryPolicy<HttpExchange>> retryPolicyForCall = defaultRetryPolicy;


            AtomicInteger attempts = new AtomicInteger();
            try {
                attempts.addAndGet(1);
                if (retryPolicyForCall.isPresent()) {
                    httpExchange = Failsafe.with(List.of(retryPolicyForCall.get()))
                            .get(() -> callOnceWs(httpRequest, attempts));
                } else {
                    return callOnceWs(httpRequest, attempts);
                }
            } catch (Throwable throwable) {
                LOGGER.error("Failed to call web service after {} retries with error {} ", attempts, throwable.getMessage());
                return buildHttpExchange(
                        httpRequest,
                        new HttpResponse(SERVER_ERROR_STATUS_CODE, String.valueOf(throwable.getMessage()), BLANK_RESPONSE_CONTENT),
                        Stopwatch.createUnstarted(), OffsetDateTime.now(ZoneId.of(UTC_ZONE_ID)),
                        attempts,
                        FAILURE);
            }
        }
        return httpExchange;
    }

    private RetryPolicy<HttpExchange> buildRetryPolicy(Integer retries,
                                                       Long retryDelayInMs,
                                                       Long retryMaxDelayInMs,
                                                       Double retryDelayFactor,
                                                       Long retryJitterInMs) {
        RetryPolicy<HttpExchange> retryPolicy = RetryPolicy.<HttpExchange>builder()
                //we retry only if the error comes from the WS server (server-side technical error)
                .handle(HttpException.class)
                .withBackoff(Duration.ofMillis(retryDelayInMs), Duration.ofMillis(retryMaxDelayInMs), retryDelayFactor)
                .withJitter(Duration.ofMillis(retryJitterInMs))
                .withMaxRetries(retries)
                .onRetry(listener -> LOGGER.warn("Retry ws call result:{}, failure:{}", listener.getLastResult(), listener.getLastException()))
                .onFailure(listener -> LOGGER.warn("ws call failed ! result:{},exception:{}", listener.getResult(), listener.getException()))
                .onAbort(listener -> LOGGER.warn("ws call aborted ! result:{},exception:{}", listener.getResult(), listener.getException()))
                .build();
        return retryPolicy;
    }


    protected HttpExchange callOnceWs(HttpRequest httpRequest, AtomicInteger attempts) throws HttpException {
        Pattern successPattern = getSuccessPattern(httpRequest);
        Request request = buildRequest(httpRequest);
        LOGGER.info("request: {}", request.toString());
        LOGGER.info("body: {}", request.getStringData() != null ? request.getStringData() : "");
        try {
            this.defaultRateLimiter.acquirePermits(ONE_HTTP_REQUEST);
        } catch (InterruptedException e) {
            LOGGER.error("Failed to acquire execution permit from the rate limiter {} ", e.getMessage());
            throw new HttpException(e.getMessage());
        }
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {

            OffsetDateTime now = OffsetDateTime.now(ZoneId.of(UTC_ZONE_ID));
            ListenableFuture<Response> responseListenableFuture = asyncHttpClient.executeRequest(request, asyncCompletionHandler);
            //we cannot use the asynchronous nature of the response yet
            Response response = responseListenableFuture.get();
            LOGGER.info("response: {}", response);
            stopwatch.stop();

            HttpResponse httpResponse = buildResponse(response, successPattern);
            LOGGER.info("duration: {}", stopwatch);
            return getHttpExchange(httpRequest, httpResponse, stopwatch, now, attempts);
        } catch (HttpException | InterruptedException | ExecutionException e) {
            LOGGER.error("Failed to call web service {} ", e.getMessage());
            throw new HttpException(e.getMessage());
        } finally {
            if (stopwatch.isRunning()) {
                stopwatch.stop();
            }
        }


    }

    private HttpResponse buildResponse(Response response, Pattern successPattern) {
        int responseStatusCode = getSuccessfulStatusCodeOrThrowRetryException(response.getStatusCode(), successPattern);
        List<Map.Entry<String, String>> responseEntries = response.getHeaders() != null ? response.getHeaders().entries() : Lists.newArrayList();
        HttpResponse httpResponse = new HttpResponse(responseStatusCode, response.getStatusText(), response.getResponseBody());
        Map<String, List<String>> responseHeaders = responseEntries.stream()
                .map(entry -> new AbstractMap.SimpleImmutableEntry<String, List<String>>(entry.getKey(), Lists.newArrayList(entry.getValue())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        httpResponse.setResponseHeaders(responseHeaders);
        return httpResponse;
    }

    private HttpExchange getHttpExchange(HttpRequest httpRequest,
                                         HttpResponse httpResponse,
                                         Stopwatch stopwatch,
                                         OffsetDateTime now,
                                         AtomicInteger attempts) {
        return buildHttpExchange(
                httpRequest,
                httpResponse,
                stopwatch,
                now,
                attempts,
                SUCCESS
        );
    }

    private int getSuccessfulStatusCodeOrThrowRetryException(int responseStatusCode, Pattern pattern) {
        Matcher matcher = pattern.matcher("" + responseStatusCode);

        if (!matcher.matches() && responseStatusCode >= 500) {
            throw new HttpException("response status code:" + responseStatusCode + " does not match status code success regex " + pattern.pattern());
        }
        return responseStatusCode;
    }

    private Pattern getSuccessPattern(HttpRequest httpRequest) {
        //by default, we don't resend any http call with a response between 100 and 499
        // 1xx is for protocol information (100 continue for example),
        // 2xx is for success,
        // 3xx is for redirection
        //4xx is for a client error
        //5xx is for a server error
        //only 5xx by default, trigger a resend

        /*
         *  HTTP Server status code returned
         *  3 cases can arise:
         *  * a success occurs : the status code returned from the ws server is matching the regexp => no retries
         *  * a functional error occurs: the status code returned from the ws server is not matching the regexp, but is lower than 500 => no retries
         *  * a technical error occurs from the WS server : the status code returned from the ws server does not match the regexp AND is equals or higher than 500 : retries are done
         */
        String wsSuccessCode = "^[1-4][0-9][0-9]$";

        if (this.httpSuccessCodesPatterns.get(wsSuccessCode) == null) {
            //Pattern.compile should be reused for performance, but wsSuccessCode can change....
            Pattern httpSuccessPattern = Pattern.compile(wsSuccessCode);
            httpSuccessCodesPatterns.put(wsSuccessCode, httpSuccessPattern);
        }
        Pattern pattern = httpSuccessCodesPatterns.get(wsSuccessCode);
        return pattern;
    }


    protected Request buildRequest(HttpRequest httpRequest) {
        Preconditions.checkNotNull(httpRequest, "'httpRequest' is required but null");
        Preconditions.checkNotNull(httpRequest.getHeaders(), "'headers' are required but null");
        Preconditions.checkNotNull(httpRequest.getBodyAsString(), "'body' is required but null");
        String url = httpRequest.getUrl();
        Preconditions.checkNotNull(url, "'url' is required but null");
        String method = httpRequest.getMethod();
        Preconditions.checkNotNull(method, "'method' is required but null");

        //extract http headers
        Map<String, List<String>> httpHeaders = httpRequest.getHeaders();

        RequestBuilder requestBuilder = new RequestBuilder()
                .setUrl(url)
                .setHeaders(httpHeaders)
                .setMethod(method)
                .setBody(httpRequest.getBodyAsString());

        //extract proxy headers
        Map<String, String> proxyHeaders = Maps.newHashMap();
        httpHeaders.entrySet().stream().filter(entry -> entry.getKey().startsWith(PROXY_PREFIX))
                .forEach(entry -> proxyHeaders.put(entry.getKey().substring(PROXY_PREFIX.length()), entry.getValue().get(0)));

        defineProxyServer(requestBuilder, proxyHeaders);

        //extract proxy headers
        Map<String, String> realmHeaders = Maps.newHashMap();
        httpHeaders.entrySet().stream().filter(entry -> entry.getKey().startsWith(PROXY_PREFIX))
                .forEach(entry -> realmHeaders.put(entry.getKey().substring(REALM_PREFIX.length()), entry.getValue().get(0)));

        defineRealm(requestBuilder, proxyHeaders);
        //retry stuff
        int requestTimeoutInMillis;
        if (httpHeaders.get(WS_REQUEST_TIMEOUT_IN_MS) != null) {
            requestTimeoutInMillis = Integer.parseInt(httpHeaders.get(WS_REQUEST_TIMEOUT_IN_MS).get(0));
            requestBuilder.setRequestTimeout(requestTimeoutInMillis);
        }

        int readTimeoutInMillis;
        if (httpHeaders.get(WS_READ_TIMEOUT_IN_MS) != null) {
            readTimeoutInMillis = Integer.parseInt(httpHeaders.get(WS_READ_TIMEOUT_IN_MS).get(0));
            requestBuilder.setReadTimeout(readTimeoutInMillis);
        }
        return requestBuilder.build();
    }

    private void defineProxyServer(RequestBuilder requestBuilder, Map<String, String> proxyHeaders) {
        //proxy stuff
        String proxyHost = proxyHeaders.get(WS_PROXY_HOST);
        if (proxyHost != null) {
            int proxyPort = Integer.parseInt(proxyHeaders.get(WS_PROXY_PORT));
            ProxyServer.Builder proxyBuilder = new ProxyServer.Builder(proxyHost, proxyPort);

            String securedPortAsString = proxyHeaders.get(WS_PROXY_SECURED_PORT);
            if (securedPortAsString != null) {
                int securedProxyPort = Integer.parseInt(securedPortAsString);
                proxyBuilder.setSecuredPort(securedProxyPort);
            }

            String nonProxyHosts = proxyHeaders.get(WS_PROXY_NON_PROXY_HOSTS);

            if (isNotNullOrEmpty(nonProxyHosts)) {
                List<String> nonProxyHostAsList = Lists.newArrayList(nonProxyHosts.split(","));
                proxyBuilder.setNonProxyHosts(nonProxyHostAsList);
            }
            proxyBuilder.setProxyType(ProxyType.valueOf(Optional.ofNullable(proxyHeaders.get(WS_PROXY_TYPE)).orElse(HTTP_PROXY_TYPE)));

            requestBuilder.setProxyServer(proxyBuilder.build());
        }
    }

    private void defineRealm(RequestBuilder requestBuilder, Map<String, String> realmHeaders) {
        String principals = realmHeaders.get(WS_REALM_PRINCIPAL_NAME);
        String passwords = realmHeaders.get(WS_REALM_PASS);
        if (isNotNullOrEmpty(principals)
                && isNotNullOrEmpty(passwords)) {
            Realm.Builder realmBuilder = new Realm.Builder(principals, passwords);

            realmBuilder.setAlgorithm(realmHeaders.get(WS_REALM_ALGORITHM));

            realmBuilder.setCharset(Charset.forName(Optional.ofNullable(realmHeaders.get(WS_REALM_CHARSET)).orElse(UTF_8)));

            realmBuilder.setLoginContextName(realmHeaders.get(WS_REALM_LOGIN_CONTEXT_NAME));

            String authSchemeAsString = realmHeaders.get(WS_REALM_AUTH_SCHEME);
            Realm.AuthScheme authScheme = authSchemeAsString != null ? Realm.AuthScheme.valueOf(authSchemeAsString) : null;

            realmBuilder.setScheme(authScheme);

            realmBuilder.setMethodName(Optional.ofNullable(realmHeaders.get(WS_REALM_METHOD)).orElse("GET"));

            realmBuilder.setNc(realmHeaders.get(WS_REALM_NC));

            realmBuilder.setNonce((realmHeaders.get(WS_REALM_NONCE)));

            realmBuilder.setNtlmDomain(Optional.ofNullable(realmHeaders.get(WS_REALM_NTLM_DOMAIN)).orElse(System.getProperty("http.auth.ntlm.domain")));

            realmBuilder.setNtlmHost(Optional.ofNullable(realmHeaders.get(WS_REALM_NTLM_HOST)).orElse("localhost"));

            realmBuilder.setOmitQuery(Boolean.parseBoolean(realmHeaders.get(WS_REALM_OMIT_QUERY)));

            realmBuilder.setOpaque(realmHeaders.get(WS_REALM_OPAQUE));

            realmBuilder.setQop(realmHeaders.get(WS_REALM_QOP));

            realmBuilder.setServicePrincipalName(realmHeaders.get(WS_REALM_SERVICE_PRINCIPAL_NAME));

            realmBuilder.setRealmName(realmHeaders.get(WS_REALM_NAME));

            realmBuilder.setUsePreemptiveAuth(Boolean.parseBoolean(realmHeaders.get(WS_REALM_USE_PREEMPTIVE_AUTH)));

            requestBuilder.setRealm(realmBuilder.build());
        }
    }

    private boolean isNotNullOrEmpty(String field) {
        return field != null && !field.isEmpty();
    }


    protected HttpExchange buildHttpExchange(HttpRequest httpRequest,
                                             HttpResponse httpResponse,
                                             Stopwatch stopwatch,
                                             OffsetDateTime now,
                                             AtomicInteger attempts,
                                             boolean success) {
        Preconditions.checkNotNull(httpRequest, "'httpRequest' is null");
        return HttpExchange.Builder.anHttpExchange()
                //request
                .withHttpRequest(httpRequest)
                //response
                .withHttpResponse(httpResponse)
                //technical metadata
                //time elapsed during http call
                .withDuration(stopwatch.elapsed(TimeUnit.MILLISECONDS))
                //at which moment occurs the beginning of the http call
                .at(now)
                .withAttempts(attempts)
                .withSuccess(success)
                .build();
    }

    protected void setAsyncHttpClient(AsyncHttpClient asyncHttpClient) {
        this.asyncHttpClient = asyncHttpClient;
    }

    public void setDefaultRetryPolicy(Integer retries, Long retryDelayInMs, Long retryMaxDelayInMs, Double retryDelayFactor, Long retryJitterInMs) {
        this.defaultRetryPolicy = Optional.of(buildRetryPolicy(retries, retryDelayInMs, retryMaxDelayInMs, retryDelayFactor, retryJitterInMs));
    }

    public void setDefaultRateLimiter(long periodInMs, long maxExecutions) {
        this.defaultRateLimiter = RateLimiter.<HttpExchange>smoothBuilder(maxExecutions, Duration.of(periodInMs, ChronoUnit.MILLIS)).build();
    }
}
