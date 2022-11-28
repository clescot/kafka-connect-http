package com.github.clescot.kafka.connect.http.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.clescot.kafka.connect.http.HttpExchange;
import com.github.clescot.kafka.connect.http.HttpRequest;
import com.github.clescot.kafka.connect.http.QueueFactory;
import com.github.clescot.kafka.connect.http.sink.client.HttpClient;
import com.github.clescot.kafka.connect.http.sink.client.PropertyBasedASyncHttpClientConfig;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
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
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.asynchttpclient.config.AsyncHttpClientConfigDefaults.ASYNC_CLIENT_CONFIG_ROOT;


public class HttpSinkTask extends SinkTask {
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
    private final static Logger LOGGER = LoggerFactory.getLogger(HttpSinkTask.class);
    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    public static final String HEADER_X_CORRELATION_ID = "X-Correlation-ID";
    public static final String HEADER_X_REQUEST_ID = "X-Request-ID";
    private HttpClient httpClient;
    private Queue<HttpExchange> queue;
    private String queueName;

    private static AsyncHttpClient asyncHttpClient;
    private Map<String, List<String>> staticRequestHeaders;
    private HttpSinkConnectorConfig httpSinkConnectorConfig;
    private ErrantRecordReporter errantRecordReporter;
    private boolean generateMissingCorrelationId;
    private boolean generateMissingRequestId;

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
     * @param settings
     */
    @Override
    public void start(Map<String, String> settings) {
        Preconditions.checkNotNull(settings, "settings cannot be null");
        try {
            //TODO handle DLQ with errantRecordReporter
            errantRecordReporter = context.errantRecordReporter();
            if(errantRecordReporter==null){
                LOGGER.warn("Dead Letter Queue (DLQ) is not enabled. it is recommended to configure a Dead Letter Queue for a better error handling.");
            }
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            LOGGER.warn("errantRecordReporter has been added to Kafka Connect since 2.6.0 release. you should upgrade the Kafka Connect Runtime shortly.");
            errantRecordReporter = null;
        }

        this.httpSinkConnectorConfig = new HttpSinkConnectorConfig(HttpSinkConfigDefinition.config(), settings);

        this.queueName = httpSinkConnectorConfig.getQueueName();
        this.queue = QueueFactory.getQueue(queueName);
        this.staticRequestHeaders = httpSinkConnectorConfig.getStaticRequestHeaders();
        this.generateMissingRequestId = httpSinkConnectorConfig.isGenerateMissingRequestId();
        this.generateMissingCorrelationId = httpSinkConnectorConfig.isGenerateMissingCorrelationId();

        this.httpClient = new HttpClient(getAsyncHttpClient(httpSinkConnectorConfig.originalsStrings()));
        Integer defaultRetries = httpSinkConnectorConfig.getDefaultRetries();
        Long defaultRetryDelayInMs = httpSinkConnectorConfig.getDefaultRetryDelayInMs();
        Long defaultRetryMaxDelayInMs = httpSinkConnectorConfig.getDefaultRetryMaxDelayInMs();
        Double defaultRetryDelayFactor = httpSinkConnectorConfig.getDefaultRetryDelayFactor();
        Long defaultRetryJitterInMs = httpSinkConnectorConfig.getDefaultRetryJitterInMs();

           httpClient.setDefaultRetryPolicy(
                   defaultRetries,
                   defaultRetryDelayInMs,
                   defaultRetryMaxDelayInMs,
                   defaultRetryDelayFactor,
                   defaultRetryJitterInMs
           );
        }


    private static synchronized AsyncHttpClient getAsyncHttpClient(Map<String, String> config) {
        if (asyncHttpClient == null) {
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

            asyncHttpClient = Dsl.asyncHttpClient(propertyBasedASyncHttpClientConfig);
        }
        return asyncHttpClient;
    }


    @Override
    public void put(Collection<SinkRecord> records) {

        Preconditions.checkNotNull(records, "records collection to be processed is null");
        if (records.isEmpty()) {
            return;
        }
        Preconditions.checkNotNull(httpClient, "httpClient is null. 'start' method must be called once before put");
        if (httpSinkConnectorConfig.isPublishToInMemoryQueue()) {
            Preconditions.checkArgument(QueueFactory.hasAConsumer(queueName), "'" + queueName + "' queue hasn't got any consumer, i.e no Source Connector has been configured to consume records published in this in memory queue. we stop the Sink Connector to prevent any OutofMemoryError.");
        }
        for (SinkRecord sinkRecord:records) {
             try {
                 // attempt to send record to data sink
                 process(sinkRecord);
             } catch (Exception e) {
                 if (errantRecordReporter != null) {
                     // Send errant record to error reporter
                     Future<Void> future = errantRecordReporter.report(sinkRecord, e);
                     // Optionally wait till the failure's been recorded in Kafka
                     try {
                         future.get();
                     } catch (InterruptedException |ExecutionException ex) {
                         throw new RuntimeException(ex);
                     }
                 } else {
                     // There's no error reporter, so fail
                     throw new ConnectException("Failed on record", e);
                 }
             }
        }

    }

    private void process(SinkRecord sinkRecord) {
        if(sinkRecord.value()==null){
            throw new ConnectException("sinkRecord Value is null :"+ sinkRecord);
        }
        HttpRequest httpRequest = buildHttpRequest(sinkRecord);
        HttpRequest httpRequestWithStaticHeaders = addStaticHeaders(httpRequest);
        HttpRequest httpRequestWithTrackingHeaders = addTrackingHeaders(httpRequestWithStaticHeaders);
        HttpExchange httpExchange = httpClient.call(httpRequestWithTrackingHeaders);
        LOGGER.debug("HTTP exchange :{}", httpExchange);
        if (httpSinkConnectorConfig.isPublishToInMemoryQueue() && httpExchange != null) {
            LOGGER.debug("http exchange published to queue :{}", httpExchange);
            queue.offer(httpExchange);
        } else {
            LOGGER.debug("http exchange NOT published to queue :{}", httpExchange);
        }
    }

    private  HttpRequest addTrackingHeaders(HttpRequest httpRequest) {
        if(httpRequest==null){
            LOGGER.warn("sinkRecord has got a 'null' value");
            throw new ConnectException("sinkRecord has got a 'null' value");
        }
        Map<String, List<String>> headers = Optional.ofNullable(httpRequest.getHeaders()).orElse(Maps.newHashMap());

        //we generate an 'X-Request-ID' header if not present
        Optional<List<String>> requestId = Optional.ofNullable(httpRequest.getHeaders().get(HEADER_X_REQUEST_ID));
        if(requestId.isEmpty()&&this.generateMissingRequestId){
            requestId = Optional.of(Lists.newArrayList(UUID.randomUUID().toString()));
        }
        requestId.ifPresent(reqId -> headers.put(HEADER_X_REQUEST_ID, Lists.newArrayList(reqId)));

        //we generate an 'X-Correlation-ID' header if not present
        Optional<List<String>> correlationId = Optional.ofNullable(httpRequest.getHeaders().get(HEADER_X_CORRELATION_ID));
        if(correlationId.isEmpty()&&this.generateMissingCorrelationId){
            correlationId = Optional.of(Lists.newArrayList(UUID.randomUUID().toString()));
        }
        correlationId.ifPresent(corrId -> headers.put(HEADER_X_CORRELATION_ID, Lists.newArrayList(corrId)));

        return httpRequest;
    }

    protected HttpRequest buildHttpRequest(SinkRecord sinkRecord) {
        if(sinkRecord==null||sinkRecord.value()==null){
            LOGGER.warn("sinkRecord has got a 'null' value");
            throw new ConnectException("sinkRecord has got a 'null' value");
        }
        HttpRequest httpRequest = null;
        Object value = sinkRecord.value();
        String stringValue = null;
        try {
            Class<?> valueClass = value.getClass();
            LOGGER.debug("valueClass is {}", valueClass.getName());
            if (Struct.class.isAssignableFrom(valueClass)) {
                Struct valueAsStruct = (Struct) value;
                if (sinkRecord.valueSchema() != null) {
                    LOGGER.debug("valueSchema is {}", sinkRecord.valueSchema());
                    if (!HttpRequest.SCHEMA.equals(sinkRecord.valueSchema())) {
                        LOGGER.warn("sinkRecord has got a value Schema different from the HttpRequest Schema:{}", sinkRecord.valueSchema().name());
                    }
                }
                valueAsStruct.validate();
                httpRequest = HttpRequest
                        .Builder
                        .anHttpRequest()
                        .withStruct(valueAsStruct)
                        .build();

            } else if ("[B".equals(valueClass.getName())) {
                //we assume the value is a byte array
                stringValue = new String((byte[]) value, Charsets.UTF_8);
            } else if (String.class.isAssignableFrom(valueClass)) {
                stringValue = (String) value;
            } else {
                LOGGER.warn("value is an instance of the class " + valueClass.getName() + " not handled by the WsSinkTask");
                throw new ConnectException("value is an instance of the class " + valueClass.getName() + " not handled by the WsSinkTask");
            }
            if (httpRequest == null) {
                httpRequest = parseHttpRequestAsJsonString(stringValue);
                LOGGER.debug("successful httpRequest parsing :{}", httpRequest);
            }
        } catch (ConnectException connectException) {
            LOGGER.warn("error in sinkRecord's structure : " + sinkRecord, connectException);
            if (errantRecordReporter != null) {
                errantRecordReporter.report(sinkRecord, connectException);
            } else {
                LOGGER.warn("errantRecordReporter has been added to Kafka Connect since 2.6.0 release. you should upgrade the Kafka Connect Runtime shortly.");
                throw connectException;
            }

        }
        return httpRequest;
    }

    private HttpRequest parseHttpRequestAsJsonString(String value) throws ConnectException {
        HttpRequest httpRequest;
        try {
            httpRequest = OBJECT_MAPPER.readValue(value, HttpRequest.class);
        } catch (JsonProcessingException e) {
            LOGGER.error(e.getMessage(), e);
            throw new ConnectException(e);
        }
        return httpRequest;
    }

    private HttpRequest addStaticHeaders(HttpRequest httpRequest) {
        if (httpRequest != null) {
            this.staticRequestHeaders.forEach((key, value) -> httpRequest.getHeaders().put(key, value));
        }else{

        }
        return httpRequest;
    }


    @Override
    public void stop() {
        //Producer are stopped in connector stop
    }

    //for testing purpose
    protected void setHttpClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    //for testing purpose
    protected Map<String, List<String>> getStaticRequestHeaders() {
        //we return a copy
        return Maps.newHashMap(staticRequestHeaders);
    }

    protected void setQueue(Queue<HttpExchange> queue) {
        this.queue = queue;
    }
}
