package io.github.clescot.kafka.connect.http.sink.client.ahc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.netty.util.Timer;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.Realm;
import org.asynchttpclient.SslEngineFactory;
import org.asynchttpclient.channel.ChannelPool;
import org.asynchttpclient.channel.KeepAliveStrategy;
import org.asynchttpclient.config.AsyncHttpClientConfigDefaults;
import org.asynchttpclient.cookie.CookieStore;
import org.asynchttpclient.filter.IOExceptionFilter;
import org.asynchttpclient.filter.RequestFilter;
import org.asynchttpclient.filter.ResponseFilter;
import org.asynchttpclient.netty.channel.ConnectionSemaphoreFactory;
import org.asynchttpclient.proxy.ProxyServerSelector;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

import static org.asynchttpclient.config.AsyncHttpClientConfigDefaults.*;

public class PropertyBasedASyncHttpClientConfig implements AsyncHttpClientConfig {

    private final Properties properties;
    private final ConcurrentHashMap<String, String> propsCache = new ConcurrentHashMap<>();
    private ThreadFactory threadFactory;
    private ProxyServerSelector ProxyServerSelector;
    private SslContext sslContext;
    private Realm realm;
    private List<RequestFilter> requestFilters = Lists.newLinkedList();
    private List<ResponseFilter> responseFilters = Lists.newLinkedList();
    private List<IOExceptionFilter> ioExceptionFilters = Lists.newLinkedList();
    private CookieStore cookieStore;
    private SslEngineFactory sslEngineFactory;
    private Map<ChannelOption<Object>, Object> channelOptions = Maps.newHashMap();
    private EventLoopGroup eventLoopGroup;
    private Consumer<Channel> httpAdditionalChannelInitializer;
    private Consumer<Channel> wsAdditionalChannelInitializer;
    private ResponseBodyPartFactory responseBodyPartFactory;
    private ChannelPool channelPool;
    private ConnectionSemaphoreFactory connectionSemaphoreFactory;
    private Timer nettyTimer;
    private KeepAliveStrategy keepAliveStrategy;
    private ByteBufAllocator byteBufAllocator;




    public PropertyBasedASyncHttpClientConfig(Properties properties) {
        this.properties = properties;
    }

    @Override
    public String getAhcVersion() {
        return AsyncHttpClientConfigDefaults.AHC_VERSION;
    }

    @Override
    public String getThreadPoolName() {
        return Optional.ofNullable(getString(ASYNC_CLIENT_CONFIG_ROOT + THREAD_POOL_NAME_CONFIG)).orElse(defaultThreadPoolName());
    }

    @Override
    public int getMaxConnections() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + MAX_CONNECTIONS_CONFIG,defaultMaxConnections());
    }

    @Override
    public int getMaxConnectionsPerHost() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + MAX_CONNECTIONS_PER_HOST_CONFIG,defaultMaxConnectionsPerHost());
    }

    @Override
    public int getAcquireFreeChannelTimeout() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + ACQUIRE_FREE_CHANNEL_TIMEOUT,defaultAcquireFreeChannelTimeout());
    }

    @Override
    public int getConnectTimeout() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + CONNECTION_TIMEOUT_CONFIG,defaultConnectTimeout());
    }

    @Override
    public int getReadTimeout() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + READ_TIMEOUT_CONFIG,defaultReadTimeout());
    }

    @Override
    public int getPooledConnectionIdleTimeout() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + POOLED_CONNECTION_IDLE_TIMEOUT_CONFIG,defaultPooledConnectionIdleTimeout());
    }

    @Override
    public int getConnectionPoolCleanerPeriod() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + CONNECTION_POOL_CLEANER_PERIOD_CONFIG,defaultConnectionPoolCleanerPeriod());
    }

    @Override
    public int getRequestTimeout() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + REQUEST_TIMEOUT_CONFIG,defaultRequestTimeout());
    }

    @Override
    public boolean isFollowRedirect() {
        return getBoolean(ASYNC_CLIENT_CONFIG_ROOT + FOLLOW_REDIRECT_CONFIG,defaultFollowRedirect());
    }

    @Override
    public int getMaxRedirects() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + MAX_REDIRECTS_CONFIG,defaultMaxRedirects());
    }

    @Override
    public boolean isKeepAlive() {
        return getBoolean(ASYNC_CLIENT_CONFIG_ROOT + KEEP_ALIVE_CONFIG,defaultKeepAlive());
    }

    @Override
    public String getUserAgent() {
        return Optional.ofNullable(getString(ASYNC_CLIENT_CONFIG_ROOT + USER_AGENT_CONFIG)).orElse(defaultUserAgent());
    }

    @Override
    public boolean isCompressionEnforced() {
        return getBoolean(ASYNC_CLIENT_CONFIG_ROOT + COMPRESSION_ENFORCED_CONFIG,defaultCompressionEnforced());
    }

    @Override
    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }

    @Override
    public ProxyServerSelector getProxyServerSelector() {
        return ProxyServerSelector;
    }

    @Override
    public SslContext getSslContext() {
        return sslContext;
    }

    @Override
    public Realm getRealm() {
        return realm;
    }

    @Override
    public List<RequestFilter> getRequestFilters() {
        return requestFilters;
    }

    @Override
    public List<ResponseFilter> getResponseFilters() {
        return responseFilters;
    }

    @Override
    public List<IOExceptionFilter> getIoExceptionFilters() {
        return ioExceptionFilters;
    }

    @Override
    public CookieStore getCookieStore() {
        return cookieStore;
    }

    @Override
    public int expiredCookieEvictionDelay() {
        return 0;
    }

    @Override
    public int getMaxRequestRetry() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + MAX_REQUEST_RETRY_CONFIG,defaultMaxRequestRetry());
    }

    @Override
    public boolean isDisableUrlEncodingForBoundRequests() {
        return getBoolean(ASYNC_CLIENT_CONFIG_ROOT + DISABLE_URL_ENCODING_FOR_BOUND_REQUESTS_CONFIG,defaultDisableUrlEncodingForBoundRequests());
    }

    @Override
    public boolean isUseLaxCookieEncoder() {
        return getBoolean(ASYNC_CLIENT_CONFIG_ROOT + USE_LAX_COOKIE_ENCODER_CONFIG,defaultUseLaxCookieEncoder());
    }

    @Override
    public boolean isStrict302Handling() {
        return getBoolean(ASYNC_CLIENT_CONFIG_ROOT + STRICT_302_HANDLING_CONFIG,defaultStrict302Handling());
    }

    @Override
    public int getConnectionTtl() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + CONNECTION_TTL_CONFIG,defaultConnectionTtl());
    }

    @Override
    public boolean isUseOpenSsl() {
        return getBoolean(ASYNC_CLIENT_CONFIG_ROOT + USE_OPEN_SSL_CONFIG,defaultUseOpenSsl());
    }

    @Override
    public boolean isUseInsecureTrustManager() {
        return getBoolean(ASYNC_CLIENT_CONFIG_ROOT + USE_INSECURE_TRUST_MANAGER_CONFIG,defaultUseInsecureTrustManager());
    }

    @Override
    public boolean isDisableHttpsEndpointIdentificationAlgorithm() {
        return getBoolean(ASYNC_CLIENT_CONFIG_ROOT + DISABLE_HTTPS_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,defaultDisableHttpsEndpointIdentificationAlgorithm());
    }

    @Override
    public String[] getEnabledProtocols() {
        return getStringArray(ASYNC_CLIENT_CONFIG_ROOT + ENABLED_PROTOCOLS_CONFIG,defaultEnabledProtocols());
    }

    @Override
    public String[] getEnabledCipherSuites() {
        return getStringArray(ASYNC_CLIENT_CONFIG_ROOT + ENABLED_CIPHER_SUITES_CONFIG,defaultEnabledCipherSuites());
    }

    @Override
    public boolean isFilterInsecureCipherSuites() {
        return getBoolean(ASYNC_CLIENT_CONFIG_ROOT + FILTER_INSECURE_CIPHER_SUITES_CONFIG,defaultFilterInsecureCipherSuites());
    }

    @Override
    public int getSslSessionCacheSize() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + SSL_SESSION_CACHE_SIZE_CONFIG,defaultSslSessionCacheSize());
    }

    @Override
    public int getSslSessionTimeout() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + SSL_SESSION_TIMEOUT_CONFIG,defaultSslSessionTimeout());
    }

    @Override
    public int getHttpClientCodecMaxInitialLineLength() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + HTTP_CLIENT_CODEC_MAX_INITIAL_LINE_LENGTH_CONFIG,defaultHttpClientCodecMaxInitialLineLength());
    }

    @Override
    public int getHttpClientCodecMaxHeaderSize() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + HTTP_CLIENT_CODEC_MAX_HEADER_SIZE_CONFIG,defaultHttpClientCodecMaxHeaderSize());
    }

    @Override
    public int getHttpClientCodecMaxChunkSize() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + HTTP_CLIENT_CODEC_MAX_CHUNK_SIZE_CONFIG,defaultHttpClientCodecMaxChunkSize());
    }

    @Override
    public int getHttpClientCodecInitialBufferSize() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + HTTP_CLIENT_CODEC_INITIAL_BUFFER_SIZE_CONFIG,defaultHttpClientCodecInitialBufferSize());
    }

    @Override
    public boolean isDisableZeroCopy() {
        return getBoolean(ASYNC_CLIENT_CONFIG_ROOT + DISABLE_ZERO_COPY_CONFIG,defaultDisableZeroCopy());
    }

    @Override
    public int getHandshakeTimeout() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + HANDSHAKE_TIMEOUT_CONFIG,defaultHandshakeTimeout());
    }

    @Override
    public SslEngineFactory getSslEngineFactory() {
        return sslEngineFactory;
    }

    @Override
    public int getChunkedFileChunkSize() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + CHUNKED_FILE_CHUNK_SIZE_CONFIG,defaultChunkedFileChunkSize());
    }

    @Override
    public int getWebSocketMaxBufferSize() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + WEBSOCKET_MAX_BUFFER_SIZE_CONFIG,defaultWebSocketMaxBufferSize());
    }

    @Override
    public int getWebSocketMaxFrameSize() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + WEBSOCKET_MAX_FRAME_SIZE_CONFIG,defaultWebSocketMaxFrameSize());
    }

    @Override
    public boolean isKeepEncodingHeader() {
        return getBoolean(ASYNC_CLIENT_CONFIG_ROOT + KEEP_ENCODING_HEADER_CONFIG,defaultKeepEncodingHeader());
    }

    @Override
    public int getShutdownQuietPeriod() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + SHUTDOWN_QUIET_PERIOD_CONFIG,defaultShutdownQuietPeriod());
    }

    @Override
    public int getShutdownTimeout() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + SHUTDOWN_TIMEOUT_CONFIG,defaultShutdownTimeout());
    }

    @Override
    public Map<ChannelOption<Object>, Object> getChannelOptions() {
        return channelOptions;
    }

    @Override
    public EventLoopGroup getEventLoopGroup() {
        return eventLoopGroup;
    }

    @Override
    public boolean isUseNativeTransport() {
        return getBoolean(ASYNC_CLIENT_CONFIG_ROOT + USE_NATIVE_TRANSPORT_CONFIG,defaultUseNativeTransport());
    }

    @Override
    public Consumer<Channel> getHttpAdditionalChannelInitializer() {
        return httpAdditionalChannelInitializer;
    }

    @Override
    public Consumer<Channel> getWsAdditionalChannelInitializer() {
        return wsAdditionalChannelInitializer;
    }

    @Override
    public ResponseBodyPartFactory getResponseBodyPartFactory() {
        return responseBodyPartFactory;
    }

    @Override
    public ChannelPool getChannelPool() {
        return channelPool;
    }

    @Override
    public ConnectionSemaphoreFactory getConnectionSemaphoreFactory() {
        return connectionSemaphoreFactory;
    }

    @Override
    public Timer getNettyTimer() {
        return nettyTimer;
    }

    @Override
    public long getHashedWheelTimerTickDuration() {
        return 0;
    }

    @Override
    public int getHashedWheelTimerSize() {
        return 0;
    }

    @Override
    public KeepAliveStrategy getKeepAliveStrategy() {
        return keepAliveStrategy;
    }

    @Override
    public boolean isValidateResponseHeaders() {
        return getBoolean(ASYNC_CLIENT_CONFIG_ROOT + VALIDATE_RESPONSE_HEADERS_CONFIG,defaultValidateResponseHeaders());
    }

    @Override
    public boolean isAggregateWebSocketFrameFragments() {
        return getBoolean(ASYNC_CLIENT_CONFIG_ROOT + AGGREGATE_WEBSOCKET_FRAME_FRAGMENTS_CONFIG,defaultAggregateWebSocketFrameFragments());
    }

    @Override
    public boolean isEnableWebSocketCompression() {
        return getBoolean(ASYNC_CLIENT_CONFIG_ROOT + ENABLE_WEBSOCKET_COMPRESSION_CONFIG,defaultEnableWebSocketCompression());
    }

    @Override
    public boolean isTcpNoDelay() {
        return getBoolean(ASYNC_CLIENT_CONFIG_ROOT + TCP_NO_DELAY_CONFIG,defaultTcpNoDelay());
    }

    @Override
    public boolean isSoReuseAddress() {
        return getBoolean(ASYNC_CLIENT_CONFIG_ROOT + SO_REUSE_ADDRESS_CONFIG,defaultSoReuseAddress());
    }

    @Override
    public boolean isSoKeepAlive() {
        return false;
    }

    @Override
    public int getSoLinger() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + SO_LINGER_CONFIG,defaultSoLinger());
    }

    @Override
    public int getSoSndBuf() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + SO_SND_BUF_CONFIG,defaultSoSndBuf());
    }

    @Override
    public int getSoRcvBuf() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + SO_RCV_BUF_CONFIG,defaultSoRcvBuf());
    }

    @Override
    public ByteBufAllocator getAllocator() {
        return byteBufAllocator;
    }

    @Override
    public int getIoThreadsCount() {
        return getInt(ASYNC_CLIENT_CONFIG_ROOT + IO_THREADS_COUNT_CONFIG,defaultIoThreadsCount());
    }


    public String getString(String key) {
        return propsCache.computeIfAbsent(key, k -> {
            String value = System.getProperty(k);
            if (value == null)
                value = properties.getProperty(k);
            return value;
        });
    }

    public String[] getStringArray(String key,String[] defaultValue) {
        String s = getString(key);
        if(s==null){
            return defaultValue;
        }
        s = s.trim();
        if (s.isEmpty()) {
            return defaultValue;
        }
        String[] rawArray = s.split(",");
        String[] array = new String[rawArray.length];
        for (int i = 0; i < rawArray.length; i++)
            array[i] = rawArray[i].trim();
        return array;
    }

    public int getInt(String key,int defaultValue) {
        String string = getString(key);
        if(string!=null) {
            return Integer.parseInt(string);
        }else{
            return defaultValue;
        }
    }

    public boolean getBoolean(String key,boolean defaultValue) {
        String string = getString(key);
        if(string!=null) {
            return Boolean.parseBoolean(string);
        }else{
            return defaultValue;
        }
    }

    public void setIoExceptionFilters(List<IOExceptionFilter> ioExceptionFilters) {
        this.ioExceptionFilters = ioExceptionFilters;
    }

    public void setResponseFilters(List<ResponseFilter> responseFilters) {
        this.responseFilters = responseFilters;
    }

    public void setThreadFactory(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
    }

    public void setProxyServerSelector(org.asynchttpclient.proxy.ProxyServerSelector proxyServerSelector) {
        ProxyServerSelector = proxyServerSelector;
    }

    public void setSslContext(SslContext sslContext) {
        this.sslContext = sslContext;
    }

    public void setRealm(Realm realm) {
        this.realm = realm;
    }

    public void setRequestFilters(List<RequestFilter> requestFilters) {
        this.requestFilters = requestFilters;
    }

    public void setCookieStore(CookieStore cookieStore) {
        this.cookieStore = cookieStore;
    }

    public void setSslEngineFactory(SslEngineFactory sslEngineFactory) {
        this.sslEngineFactory = sslEngineFactory;
    }

    public void setChannelOptions(Map<ChannelOption<Object>, Object> channelOptions) {
        this.channelOptions = channelOptions;
    }

    public void setEventLoopGroup(EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
    }

    public void setHttpAdditionalChannelInitializer(Consumer<Channel> httpAdditionalChannelInitializer) {
        this.httpAdditionalChannelInitializer = httpAdditionalChannelInitializer;
    }

    public void setWsAdditionalChannelInitializer(Consumer<Channel> wsAdditionalChannelInitializer) {
        this.wsAdditionalChannelInitializer = wsAdditionalChannelInitializer;
    }

    public void setResponseBodyPartFactory(ResponseBodyPartFactory responseBodyPartFactory) {
        this.responseBodyPartFactory = responseBodyPartFactory;
    }

    public void setChannelPool(ChannelPool channelPool) {
        this.channelPool = channelPool;
    }

    public void setConnectionSemaphoreFactory(ConnectionSemaphoreFactory connectionSemaphoreFactory) {
        this.connectionSemaphoreFactory = connectionSemaphoreFactory;
    }

    public void setNettyTimer(Timer nettyTimer) {
        this.nettyTimer = nettyTimer;
    }

    public void setKeepAliveStrategy(KeepAliveStrategy keepAliveStrategy) {
        this.keepAliveStrategy = keepAliveStrategy;
    }

    public void setByteBufAllocator(ByteBufAllocator byteBufAllocator) {
        this.byteBufAllocator = byteBufAllocator;
    }
}
