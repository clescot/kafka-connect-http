package io.github.clescot.kafka.connect.http.client.ahc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.client.HttpClient;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.client.AbstractHttpClient;
import io.github.clescot.kafka.connect.http.client.HttpException;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import org.asynchttpclient.*;
import org.asynchttpclient.channel.DefaultKeepAliveStrategy;
import org.asynchttpclient.channel.KeepAliveStrategy;
import org.asynchttpclient.cookie.CookieStore;
import org.asynchttpclient.cookie.ThreadSafeCookieStore;
import org.asynchttpclient.extras.guava.RateLimitedThrottleRequestFilter;
import org.asynchttpclient.netty.channel.ConnectionSemaphoreFactory;
import org.asynchttpclient.netty.channel.DefaultConnectionSemaphoreFactory;
import org.asynchttpclient.proxy.ProxyServer;
import org.asynchttpclient.proxy.ProxyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;
import static org.asynchttpclient.config.AsyncHttpClientConfigDefaults.ASYNC_CLIENT_CONFIG_ROOT;

public class AHCHttpClient extends AbstractHttpClient<Request, Response> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AHCHttpClient.class);
    public static final String PROXY_PREFIX = "proxy-";
    public static final String REALM_PREFIX = "realm-";
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

    public static final String WS_REQUEST_TIMEOUT_IN_MS = "request-timeout-in-ms";
    public static final String WS_READ_TIMEOUT_IN_MS = "read-timeout-in-ms";
    private static final String WS_REALM_PASS = "password";

    public static final String ASYN_HTTP_CONFIG_PREFIX = ASYNC_CLIENT_CONFIG_ROOT;
    public static final String HTTP_MAX_CONNECTIONS = ASYN_HTTP_CONFIG_PREFIX + "http.max.connections";
    public static final String HTTP_RATE_LIMIT_PER_SECOND = ASYN_HTTP_CONFIG_PREFIX + "http.rate.limit.per.second";
    public static final String HTTP_MAX_WAIT_MS = ASYN_HTTP_CONFIG_PREFIX + "http.max.wait.ms";
    public static final String KEEP_ALIVE_STRATEGY_CLASS = ASYN_HTTP_CONFIG_PREFIX + "keep.alive.class";
    public static final String RESPONSE_BODY_PART_FACTORY = ASYN_HTTP_CONFIG_PREFIX + "response.body.part.factory";
    private static final String CONNECTION_SEMAPHORE_FACTORY = ASYN_HTTP_CONFIG_PREFIX + "connection.semaphore.factory";
    private static final String COOKIE_STORE = ASYN_HTTP_CONFIG_PREFIX + "cookie.store";
    private static final String NETTY_TIMER = ASYN_HTTP_CONFIG_PREFIX + "netty.timer";
    private static final String BYTE_BUFFER_ALLOCATOR = ASYN_HTTP_CONFIG_PREFIX + "byte.buffer.allocator";
    private final AsyncHttpClient asyncHttpClient;

    private final HttpClientAsyncCompletionHandler asyncCompletionHandler = new HttpClientAsyncCompletionHandler();

    public AHCHttpClient(Map<String, Object> config) {
        super(config);
        this.asyncHttpClient = getAsyncHttpClient(config);
    }
    //for tests only
    protected AHCHttpClient(AsyncHttpClient asyncHttpClient) {
        super(Maps.newHashMap());
        this.asyncHttpClient =asyncHttpClient;
    }

    @Override
    public CompletableFuture<Response> nativeCall(org.asynchttpclient.Request request) {
        LOGGER.debug("native call  {}",request);
        if(request.getStringData()!=null) {
            LOGGER.debug("body stringData: '{}'", request.getStringData());
        }else{
            LOGGER.debug("body stringData: null");
        }
        ListenableFuture<Response> listenableFuture = asyncHttpClient.executeRequest(request, asyncCompletionHandler);
        return listenableFuture.toCompletableFuture();


    }



    @Override
    public Request buildRequest(HttpRequest httpRequest) {
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
        String proxyHost = proxyHeaders.get(PROXY_HTTP_CLIENT_HOSTNAME);
        if (proxyHost != null) {
            int proxyPort = Integer.parseInt(proxyHeaders.get(PROXY_HTTP_CLIENT_PORT));
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
    public HttpResponse buildResponse(Response response) throws HttpException {
        List<Map.Entry<String, String>> responseEntries = response.getHeaders() != null ? response.getHeaders().entries() : Lists.newArrayList();
        HttpResponse httpResponse = new HttpResponse(response.getStatusCode(), response.getStatusText());
        httpResponse.setResponseBody(response.getResponseBody());
        Map<String, List<String>> responseHeaders = responseEntries.stream()
                .map(entry -> new AbstractMap.SimpleImmutableEntry<String, List<String>>(entry.getKey(), Lists.newArrayList(entry.getValue())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,(l1,l2)->{
                    l1.addAll(l2);
                    return l1;
                }));
        httpResponse.setResponseHeaders(responseHeaders);
        return httpResponse;
    }
    private AsyncHttpClient getAsyncHttpClient(Map<String, Object> config) {
        AsyncHttpClient asyncClient;
        Map<String, String> asyncConfig = config.entrySet().stream().filter(entry -> entry.getKey().startsWith(ASYN_HTTP_CONFIG_PREFIX))
                .map(k->Map.entry(k.getKey(),k.getValue().toString()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Properties asyncHttpProperties = new Properties();
        asyncHttpProperties.putAll(asyncConfig);
        PropertyBasedASyncHttpClientConfig propertyBasedASyncHttpClientConfig = new PropertyBasedASyncHttpClientConfig(asyncHttpProperties);
        //define throttling
        int maxConnections = Integer.parseInt(config.getOrDefault(HTTP_MAX_CONNECTIONS, "3").toString());
        double rateLimitPerSecond = Double.parseDouble(config.getOrDefault(HTTP_RATE_LIMIT_PER_SECOND, "3").toString());
        int maxWaitMs = Integer.parseInt(config.getOrDefault(HTTP_MAX_WAIT_MS, "500").toString());
        propertyBasedASyncHttpClientConfig.setRequestFilters(Lists.newArrayList(new RateLimitedThrottleRequestFilter(maxConnections, rateLimitPerSecond, maxWaitMs)));

        String defaultKeepAliveStrategyClassName = config.getOrDefault(KEEP_ALIVE_STRATEGY_CLASS, "org.asynchttpclient.channel.DefaultKeepAliveStrategy").toString();

        //define keep alive strategy
        KeepAliveStrategy keepAliveStrategy;
        try {
            //we instantiate the default constructor, public or not
            keepAliveStrategy = (KeepAliveStrategy) Class.forName(defaultKeepAliveStrategyClassName).getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException |
                 InvocationTargetException | NoSuchMethodException e) {
            LOGGER.error(e.getMessage());
            LOGGER.error("we rollback to the default keep alive strategy");
            keepAliveStrategy = new DefaultKeepAliveStrategy();
        }
        propertyBasedASyncHttpClientConfig.setKeepAliveStrategy(keepAliveStrategy);

        //set response body part factory mode
        String responseBodyPartFactoryMode = config.getOrDefault(RESPONSE_BODY_PART_FACTORY, "EAGER").toString();
        propertyBasedASyncHttpClientConfig.setResponseBodyPartFactory(AsyncHttpClientConfig.ResponseBodyPartFactory.valueOf(responseBodyPartFactoryMode));

        //define connection semaphore factory
        String connectionSemaphoreFactoryClassName = config.getOrDefault(CONNECTION_SEMAPHORE_FACTORY, "org.asynchttpclient.netty.channel.DefaultConnectionSemaphoreFactory").toString();
        try {
            propertyBasedASyncHttpClientConfig.setConnectionSemaphoreFactory((ConnectionSemaphoreFactory) Class.forName(connectionSemaphoreFactoryClassName).getDeclaredConstructor().newInstance());
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException | ClassNotFoundException e) {
            LOGGER.error(e.getMessage());
            propertyBasedASyncHttpClientConfig.setConnectionSemaphoreFactory(new DefaultConnectionSemaphoreFactory());
            LOGGER.error("we rollback to the default connection semaphore factory");
        }

        //cookie store
        String cookieStoreClassName = config.getOrDefault(COOKIE_STORE, "org.asynchttpclient.cookie.ThreadSafeCookieStore").toString();
        try {
            propertyBasedASyncHttpClientConfig.setCookieStore((CookieStore) Class.forName(cookieStoreClassName).getDeclaredConstructor().newInstance());
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException | ClassNotFoundException e) {
            LOGGER.error(e.getMessage());
            propertyBasedASyncHttpClientConfig.setCookieStore(new ThreadSafeCookieStore());
            LOGGER.error("we rollback to the default cookie store");
        }

        //netty timer
        String nettyTimerClassName = config.getOrDefault(NETTY_TIMER, "io.netty.util.HashedWheelTimer").toString();
        try {
            propertyBasedASyncHttpClientConfig.setNettyTimer((Timer) Class.forName(nettyTimerClassName).getDeclaredConstructor().newInstance());
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException | ClassNotFoundException e) {
            LOGGER.error(e.getMessage());
            propertyBasedASyncHttpClientConfig.setNettyTimer(new HashedWheelTimer());
            LOGGER.error("we rollback to the default netty timer");
        }

        //byte buffer allocator
        String byteBufferAllocatorClassName = config.getOrDefault(BYTE_BUFFER_ALLOCATOR, "io.netty.buffer.PooledByteBufAllocator").toString();
        try {
            propertyBasedASyncHttpClientConfig.setByteBufAllocator((ByteBufAllocator) Class.forName(byteBufferAllocatorClassName).getDeclaredConstructor().newInstance());
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException | ClassNotFoundException e) {
            LOGGER.error(e.getMessage());
            propertyBasedASyncHttpClientConfig.setByteBufAllocator(new PooledByteBufAllocator());
            LOGGER.error("we rollback to the default byte buffer allocator");
        }
        if (config.containsKey(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PATH) && config.containsKey(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD)) {

            Optional<TrustManagerFactory> trustManagerFactory = Optional.ofNullable(
                    HttpClient.getTrustManagerFactory(config));
            if (trustManagerFactory.isPresent()) {
                SslContext nettySSLContext;
                try {
                    nettySSLContext = SslContextBuilder.forClient().trustManager(trustManagerFactory.get()).build();
                } catch (SSLException e) {
                    throw new HttpException(e);
                }
                propertyBasedASyncHttpClientConfig.setSslContext(nettySSLContext);
            }
        }
        asyncClient = Dsl.asyncHttpClient(propertyBasedASyncHttpClientConfig);
        return asyncClient;
    }

}
