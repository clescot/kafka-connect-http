package com.github.clescot.kafka.connect.http.sink;

import com.github.clescot.kafka.connect.http.QueueFactory;
import com.github.clescot.kafka.connect.http.sink.config.PropertyBasedASyncHttpClientConfig;
import com.github.clescot.kafka.connect.http.sink.service.WsCaller;
import com.github.clescot.kafka.connect.http.sink.utils.VersionUtil;
import com.github.clescot.kafka.connect.http.source.Acknowledgement;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
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

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

import static com.github.clescot.kafka.connect.http.QueueFactory.DEFAULT_QUEUE_NAME;
import static com.github.clescot.kafka.connect.http.sink.config.ConfigConstants.QUEUE_NAME;
import static com.github.clescot.kafka.connect.http.sink.config.ConfigConstants.STATIC_REQUEST_HEADER_NAMES;
import static org.asynchttpclient.config.AsyncHttpClientConfigDefaults.ASYNC_CLIENT_CONFIG_ROOT;


public class WsSinkTask extends SinkTask {
    public static final String ASYN_HTTP_CONFIG_PREFIX = ASYNC_CLIENT_CONFIG_ROOT;
    public static final String HTTP_MAX_CONNECTIONS = ASYN_HTTP_CONFIG_PREFIX + "http.max.connections";
    public static final String HTTP_RATE_LIMIT_PER_SECOND = ASYN_HTTP_CONFIG_PREFIX + "http.rate.limit.per.second";
    public static final String HTTP_MAX_WAIT_MS = ASYN_HTTP_CONFIG_PREFIX + "http.max.wait.ms";
    public static final String KEEP_ALIVE_STRATEGY_CLASS = ASYN_HTTP_CONFIG_PREFIX + "keep.alive.class";
    public static final String RESPONSE_BODY_PART_FACTORY = ASYN_HTTP_CONFIG_PREFIX + "response.body.part.factory";
    private static final String CONNECTION_SEMAPHORE_FACTORY = ASYN_HTTP_CONFIG_PREFIX + "connection.semaphore.factory";
    ;
    private static final String EVENT_LOOP_GROUP = ASYN_HTTP_CONFIG_PREFIX + "event.loop.group";
    private static final String COOKIE_STORE = ASYN_HTTP_CONFIG_PREFIX + "cookie.store";
    private static final String NETTY_TIMER = ASYN_HTTP_CONFIG_PREFIX + "netty.timer";
    private static final String BYTE_BUFFER_ALLOCATOR = ASYN_HTTP_CONFIG_PREFIX + "byte.buffer.allocator";
    public static final String EMPTY_STRING="";
    private WsCaller wsCaller;
    private final static Logger LOGGER = LoggerFactory.getLogger(WsSinkTask.class);
    private static Queue<Acknowledgement> queue;

    private static AsyncHttpClient asyncHttpClient;
    private Map<String,String> staticRequestHeaders = Maps.newHashMap();

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }

    /**
     * rate limiter configuration :
     * org.asynchttpclient.throttle.http.max.connections simultaneous max connections
     * org.asynchttpclient.throttle.http.rate.limit.per.second max calls per second
     * org.asynchttpclient.throttle.http.max.wait.ms max wait time when call quota is reached.
     * asynchttpclient configuration is starting by : 'org.asynchttpclient.'
     *
     * @param taskConfig
     */
    @Override
    public void start(Map<String, String> taskConfig) {
        Preconditions.checkNotNull(taskConfig, "taskConfig cannot be null");
        this.wsCaller = new WsCaller(getAsyncHttpClient(taskConfig));

        String queueName = Optional.ofNullable(taskConfig.get(QUEUE_NAME)).orElse(DEFAULT_QUEUE_NAME);
        this.queue = QueueFactory.getQueue(queueName);
        Optional<String> staticRequestHeaderNames = Optional.ofNullable(taskConfig.get(STATIC_REQUEST_HEADER_NAMES));
        List<String> additionalHeaderNamesList = staticRequestHeaderNames.isEmpty()?Lists.newArrayList():Arrays.asList(staticRequestHeaderNames.get().split(","));
        for(String headerName:additionalHeaderNamesList){
            String value = taskConfig.get(headerName);
            Preconditions.checkNotNull(value,"'"+headerName+"' is not configured as a parameter.");
            staticRequestHeaders.put(headerName, value);
        }
    }


    private static synchronized AsyncHttpClient getAsyncHttpClient(Map<String, String> config) {
        if (asyncHttpClient == null) {
            Map<String, String> asyncConfig = config.entrySet().stream().filter(entry -> entry.getKey().startsWith(ASYN_HTTP_CONFIG_PREFIX)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            Properties asyncHttpProperties = new Properties();
            asyncConfig.forEach(asyncHttpProperties::put);
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

            asyncHttpClient = Dsl.asyncHttpClient(propertyBasedASyncHttpClientConfig);
        }
        return asyncHttpClient;
    }


    @Override
    public void put(Collection<SinkRecord> records) {
        Preconditions.checkNotNull(records, "records collection to be processed is null");
        Preconditions.checkNotNull(wsCaller, "wsCaller is null. 'start' method must be called once before put");
        records.stream()
                .map(wsCaller::call)
                .peek(ack->LOGGER.debug("get ack :{}",ack))
                .forEach(ack -> queue.offer(ack));
    }


    @Override
    public void stop() {
        //Producer are stopped in connector stop
    }

    public WsCaller getWsCaller() {
        return wsCaller;
    }

    protected void setWsCaller(WsCaller wsCaller){
        this.wsCaller = wsCaller;
    }
}
