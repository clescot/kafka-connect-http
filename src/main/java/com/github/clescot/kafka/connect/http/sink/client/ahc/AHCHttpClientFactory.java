package com.github.clescot.kafka.connect.http.sink.client.ahc;

import com.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition;
import com.github.clescot.kafka.connect.http.sink.client.HttpClient;
import com.github.clescot.kafka.connect.http.sink.client.HttpClientFactory;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.channel.DefaultKeepAliveStrategy;
import org.asynchttpclient.channel.KeepAliveStrategy;
import org.asynchttpclient.cookie.CookieStore;
import org.asynchttpclient.cookie.ThreadSafeCookieStore;
import org.asynchttpclient.extras.guava.RateLimitedThrottleRequestFilter;
import org.asynchttpclient.netty.channel.ConnectionSemaphoreFactory;
import org.asynchttpclient.netty.channel.DefaultConnectionSemaphoreFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;
import static org.asynchttpclient.config.AsyncHttpClientConfigDefaults.ASYNC_CLIENT_CONFIG_ROOT;

public class AHCHttpClientFactory implements HttpClientFactory {

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


    private static AHCHttpClient httpClient;

    private final static Logger LOGGER = LoggerFactory.getLogger(AHCHttpClientFactory.class);


    private static synchronized AsyncHttpClient getAsyncHttpClient(Map<String, String> config) {
        AsyncHttpClient asyncHttpClient = null;
        Map<String, String> asyncConfig = config.entrySet().stream().filter(entry -> entry.getKey().startsWith(ASYN_HTTP_CONFIG_PREFIX)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Properties asyncHttpProperties = new Properties();
        asyncHttpProperties.putAll(asyncConfig);
        PropertyBasedASyncHttpClientConfig propertyBasedASyncHttpClientConfig = new PropertyBasedASyncHttpClientConfig(asyncHttpProperties);
        //define throttling
        int maxConnections = Integer.parseInt(config.getOrDefault(HTTP_MAX_CONNECTIONS, "3"));
        double rateLimitPerSecond = Double.parseDouble(config.getOrDefault(HTTP_RATE_LIMIT_PER_SECOND, "3"));
        int maxWaitMs = Integer.parseInt(config.getOrDefault(HTTP_MAX_WAIT_MS, "500"));
        propertyBasedASyncHttpClientConfig.setRequestFilters(Lists.newArrayList(new RateLimitedThrottleRequestFilter(maxConnections, rateLimitPerSecond, maxWaitMs)));

        String defaultKeepAliveStrategyClassName = config.getOrDefault(KEEP_ALIVE_STRATEGY_CLASS, "org.asynchttpclient.channel.DefaultKeepAliveStrategy");

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
        String responseBodyPartFactoryMode = config.getOrDefault(RESPONSE_BODY_PART_FACTORY, "EAGER");
        propertyBasedASyncHttpClientConfig.setResponseBodyPartFactory(AsyncHttpClientConfig.ResponseBodyPartFactory.valueOf(responseBodyPartFactoryMode));

        //define connection semaphore factory
        String connectionSemaphoreFactoryClassName = config.getOrDefault(CONNECTION_SEMAPHORE_FACTORY, "org.asynchttpclient.netty.channel.DefaultConnectionSemaphoreFactory");
        try {
            propertyBasedASyncHttpClientConfig.setConnectionSemaphoreFactory((ConnectionSemaphoreFactory) Class.forName(connectionSemaphoreFactoryClassName).getDeclaredConstructor().newInstance());
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException | ClassNotFoundException e) {
            LOGGER.error(e.getMessage());
            propertyBasedASyncHttpClientConfig.setConnectionSemaphoreFactory(new DefaultConnectionSemaphoreFactory());
            LOGGER.error("we rollback to the default connection semaphore factory");
        }

        //cookie store
        String cookieStoreClassName = config.getOrDefault(COOKIE_STORE, "org.asynchttpclient.cookie.ThreadSafeCookieStore");
        try {
            propertyBasedASyncHttpClientConfig.setCookieStore((CookieStore) Class.forName(cookieStoreClassName).getDeclaredConstructor().newInstance());
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException | ClassNotFoundException e) {
            LOGGER.error(e.getMessage());
            propertyBasedASyncHttpClientConfig.setCookieStore(new ThreadSafeCookieStore());
            LOGGER.error("we rollback to the default cookie store");
        }

        //netty timer
        String nettyTimerClassName = config.getOrDefault(NETTY_TIMER, "io.netty.util.HashedWheelTimer");
        try {
            propertyBasedASyncHttpClientConfig.setNettyTimer((Timer) Class.forName(nettyTimerClassName).getDeclaredConstructor().newInstance());
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException | ClassNotFoundException e) {
            LOGGER.error(e.getMessage());
            propertyBasedASyncHttpClientConfig.setNettyTimer(new HashedWheelTimer());
            LOGGER.error("we rollback to the default netty timer");
        }

        //byte buffer allocator
        String byteBufferAllocatorClassName = config.getOrDefault(BYTE_BUFFER_ALLOCATOR, "io.netty.buffer.PooledByteBufAllocator");
        try {
            propertyBasedASyncHttpClientConfig.setByteBufAllocator((ByteBufAllocator) Class.forName(byteBufferAllocatorClassName).getDeclaredConstructor().newInstance());
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException | ClassNotFoundException e) {
            LOGGER.error(e.getMessage());
            propertyBasedASyncHttpClientConfig.setByteBufAllocator(new PooledByteBufAllocator());
            LOGGER.error("we rollback to the default byte buffer allocator");
        }
        if (config.containsKey(HTTPCLIENT_SSL_TRUSTSTORE_PATH) && config.containsKey(HTTPCLIENT_SSL_TRUSTSTORE_PASSWORD)) {

            Optional<TrustManagerFactory> trustManagerFactory = Optional.ofNullable(
                    HttpClientFactory.getTrustManagerFactory(
                            config.get(HTTPCLIENT_SSL_TRUSTSTORE_PATH),
                            config.get(HTTPCLIENT_SSL_TRUSTSTORE_PASSWORD).toCharArray(),
                            config.get(HTTPCLIENT_SSL_TRUSTSTORE_TYPE),
                            config.get(HTTPCLIENT_SSL_TRUSTSTORE_ALGORITHM)));
            if (trustManagerFactory.isPresent()) {
                SslContext nettySSLContext;
                try {
                    nettySSLContext = SslContextBuilder.forClient().trustManager(trustManagerFactory.get()).build();
                } catch (SSLException e) {
                    throw new RuntimeException(e);
                }
                propertyBasedASyncHttpClientConfig.setSslContext(nettySSLContext);
            }
        }
        asyncHttpClient = Dsl.asyncHttpClient(propertyBasedASyncHttpClientConfig);
        return asyncHttpClient;
    }


    @Override
    public HttpClient build(Map<String, String> config) {
        if (httpClient == null) {
            httpClient = new AHCHttpClient(getAsyncHttpClient(config));
        }
        return httpClient;
    }
}

