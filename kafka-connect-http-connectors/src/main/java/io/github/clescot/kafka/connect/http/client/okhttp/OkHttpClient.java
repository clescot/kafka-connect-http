package io.github.clescot.kafka.connect.http.client.okhttp;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.jimfs.Jimfs;
import io.github.clescot.kafka.connect.http.client.AbstractHttpClient;
import io.github.clescot.kafka.connect.http.client.Configuration;
import io.github.clescot.kafka.connect.http.client.HttpException;
import io.github.clescot.kafka.connect.http.client.okhttp.configuration.AuthenticationConfigurer;
import io.github.clescot.kafka.connect.http.client.okhttp.event.AdvancedEventListenerFactory;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.micrometer.core.instrument.binder.system.DiskSpaceMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import kotlin.Pair;
import okhttp3.*;
import okhttp3.internal.http.HttpMethod;
import okhttp3.internal.io.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.net.ssl.*;
import java.io.File;
import java.io.IOException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static io.github.clescot.kafka.connect.http.HttpTask.DEFAULT_CONFIGURATION_ID;
import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

public class OkHttpClient extends AbstractHttpClient<Request, Response> {
    private static final String PROTOCOL_SEPARATOR = ",";

    public static final String IN_MEMORY_CACHE_TYPE = "inmemory";
    public static final String DEFAULT_MAX_CACHE_ENTRIES = "10000";
    public static final String FILE_CACHE_TYPE = "file";
    public static final String CONNECTOR_NAME = "connector.name";
    public static final String CONNECTOR_TASK = "connector.task";


    private final okhttp3.OkHttpClient client;
    private static final Logger LOGGER = LoggerFactory.getLogger(OkHttpClient.class);

    private static ConnectionPool sharedConnectionPool;
    private final CompositeMeterRegistry meterRegistry;

    public OkHttpClient(Map<String, Object> config,
                        ExecutorService executorService,
                        Random random,
                        Proxy proxy,
                        ProxySelector proxySelector,
                        CompositeMeterRegistry meterRegistry) {
        super(config);
        this.meterRegistry = meterRegistry;

        okhttp3.OkHttpClient.Builder httpClientBuilder = new okhttp3.OkHttpClient.Builder();
        if (executorService != null) {
            Dispatcher dispatcher = new Dispatcher(executorService);
            httpClientBuilder.dispatcher(dispatcher);
        }

        if (proxy != null) {
            httpClientBuilder.proxy(proxy);
        }

        if (proxySelector != null) {
            httpClientBuilder.proxySelector(proxySelector);
        }

        configureConnectionPool(config, httpClientBuilder);
        //protocols
        configureProtocols(config, httpClientBuilder);
        configureSSL(config, httpClientBuilder);

        configureConnection(config, httpClientBuilder);

        //cache
        configureCache(config, httpClientBuilder);

        //authentication
        AuthenticationConfigurer authenticationConfigurer = new AuthenticationConfigurer(random);
        authenticationConfigurer.configure(config, httpClientBuilder);

        //interceptor
        httpClientBuilder.addNetworkInterceptor(new LoggingInterceptor());

        //events
        boolean includeLegacyHostTag = (boolean) config.getOrDefault(METER_REGISTRY_TAG_INCLUDE_LEGACY_HOST, false);
        boolean includeUrlPath = (boolean) config.getOrDefault(METER_REGISTRY_TAG_INCLUDE_URL_PATH, false);
        if (!meterRegistry.getRegistries().isEmpty()) {
            List<String> tags = Lists.newArrayList();
            tags.add(Configuration.CONFIGURATION_ID);
            tags.add(config.get(Configuration.CONFIGURATION_ID)!=null?(String)config.get(Configuration.CONFIGURATION_ID):DEFAULT_CONFIGURATION_ID);
            String connectorName = MDC.get(CONNECTOR_NAME);
            if(connectorName!=null) {
                tags.add(CONNECTOR_NAME);
                tags.add(connectorName);
            }

            String connectorTask = MDC.get(CONNECTOR_TASK);
            if(connectorTask!=null) {
                tags.add(CONNECTOR_TASK);
                tags.add(connectorTask);
            }


            httpClientBuilder.eventListenerFactory(new AdvancedEventListenerFactory(meterRegistry, includeLegacyHostTag, includeUrlPath
                    ,tags.toArray(new String[0])
            ));
        }
        client = httpClientBuilder.build();


    }

    private void configureProtocols(Map<String, Object> config, okhttp3.OkHttpClient.Builder httpClientBuilder) {
        if (config.containsKey(OKHTTP_PROTOCOLS)) {
            String protocolNames = config.get(OKHTTP_PROTOCOLS).toString();
            List<Protocol> protocols = Lists.newArrayList();
            List<String> strings = Lists.newArrayList(protocolNames.split(PROTOCOL_SEPARATOR));
            for (String protocolName : strings) {
                Protocol protocol = Protocol.valueOf(protocolName);
                protocols.add(protocol);
            }
            httpClientBuilder.protocols(protocols);
        }
    }

    private void configureConnection(Map<String, Object> config, okhttp3.OkHttpClient.Builder httpClientBuilder) {
        //call timeout
        if (config.containsKey(OKHTTP_CALL_TIMEOUT)) {
            int callTimeout = (Integer) config.get(OKHTTP_CALL_TIMEOUT);
            httpClientBuilder.callTimeout(callTimeout, TimeUnit.MILLISECONDS);
        }

        //connect timeout
        if (config.containsKey(OKHTTP_CONNECT_TIMEOUT)) {
            int connectTimeout = Integer.parseInt(config.get(OKHTTP_CONNECT_TIMEOUT).toString());
            httpClientBuilder.connectTimeout(connectTimeout, TimeUnit.MILLISECONDS);
        }

        //read timeout
        if (config.containsKey(OKHTTP_READ_TIMEOUT)) {
            int readTimeout = Integer.parseInt(config.get(OKHTTP_READ_TIMEOUT).toString());
            httpClientBuilder.readTimeout(readTimeout, TimeUnit.MILLISECONDS);
        }

        //write timeout
        if (config.containsKey(OKHTTP_WRITE_TIMEOUT)) {
            int writeTimeout = Integer.parseInt(config.get(OKHTTP_WRITE_TIMEOUT).toString());
            httpClientBuilder.writeTimeout(writeTimeout, TimeUnit.MILLISECONDS);
        }

        //follow redirects
        if (config.containsKey(OKHTTP_FOLLOW_REDIRECT)) {
            httpClientBuilder.followRedirects((Boolean) config.get(OKHTTP_FOLLOW_REDIRECT));
        }

        //follow https redirects
        if (config.containsKey(OKHTTP_FOLLOW_SSL_REDIRECT)) {
            httpClientBuilder.followSslRedirects((Boolean) config.get(OKHTTP_FOLLOW_SSL_REDIRECT));
        }

    }

    @SuppressWarnings("java:S5527")
    private void configureSSL(Map<String, Object> config, okhttp3.OkHttpClient.Builder httpClientBuilder) {
        //KeyManager/trustManager/SSLSocketFactory
        Optional<KeyManagerFactory> keyManagerFactoryOption = getKeyManagerFactory();
        Optional<TrustManagerFactory> trustManagerFactoryOption = getTrustManagerFactory();
        if (keyManagerFactoryOption.isPresent() || trustManagerFactoryOption.isPresent()) {
            SSLSocketFactory ssl = AbstractHttpClient.getSSLSocketFactory(keyManagerFactoryOption.orElse(null), trustManagerFactoryOption.orElse(null), "SSL");
            if (trustManagerFactoryOption.isPresent()) {
                TrustManager[] trustManagers = trustManagerFactoryOption.get().getTrustManagers();
                if (trustManagers.length > 0) {
                    httpClientBuilder.sslSocketFactory(ssl, (X509TrustManager) trustManagers[0]);
                }
            }
            if (config.containsKey(OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION) && Boolean.parseBoolean(config.get(OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION).toString())) {
                httpClientBuilder.hostnameVerifier((hostname, session) -> true);
            }
        }
    }

    private void configureConnectionPool(Map<String, Object> config, okhttp3.OkHttpClient.Builder httpClientBuilder) {
        String connectionPoolScope = config.getOrDefault(OKHTTP_CONNECTION_POOL_SCOPE, "instance").toString();
        ConnectionPool connectionPool = null;
        if ("static".equalsIgnoreCase(connectionPoolScope)) {
            if (getSharedConnectionPool() == null) {
                connectionPool = buildConnectionPool(config, connectionPool);
                setSharedConnectionPool(connectionPool);
            } else {
                connectionPool = getSharedConnectionPool();
            }
        } else {
            connectionPool = buildConnectionPool(config, connectionPool);
        }

        if (connectionPool != null) {
            httpClientBuilder.connectionPool(connectionPool);
        }

    }

    private static void setSharedConnectionPool(ConnectionPool connectionPool) {
        OkHttpClient.sharedConnectionPool = connectionPool;
    }

    private static ConnectionPool getSharedConnectionPool() {
        return OkHttpClient.sharedConnectionPool;
    }

    private static ConnectionPool buildConnectionPool(Map<String, Object> config, ConnectionPool connectionPool) {
        int maxIdleConnections = Integer.parseInt(config.getOrDefault(OKHTTP_CONNECTION_POOL_MAX_IDLE_CONNECTIONS, "0").toString());
        long keepAliveDuration = Long.parseLong(config.getOrDefault(OKHTTP_CONNECTION_POOL_KEEP_ALIVE_DURATION, "0").toString());
        if (maxIdleConnections > 0 && keepAliveDuration > 0) {
            connectionPool = new ConnectionPool(maxIdleConnections, keepAliveDuration, TimeUnit.MILLISECONDS);
        }
        return connectionPool;
    }


    private void configureCache(Map<String, Object> config, okhttp3.OkHttpClient.Builder httpClientBuilder) {
        if (config.containsKey(OKHTTP_CACHE_ACTIVATE)) {
            String cacheType = config.getOrDefault(OKHTTP_CACHE_TYPE, FILE_CACHE_TYPE).toString();
            String defaultDirectoryPath;
            if (IN_MEMORY_CACHE_TYPE.equalsIgnoreCase(cacheType)) {
                defaultDirectoryPath = "/kafka-connect-http-cache";
            } else {
                defaultDirectoryPath = "/tmp/kafka-connect-http-cache";
            }

            String directoryPath = config.getOrDefault(OKHTTP_CACHE_DIRECTORY_PATH, defaultDirectoryPath).toString();

            if (IN_MEMORY_CACHE_TYPE.equalsIgnoreCase(cacheType)) {
                try (java.nio.file.FileSystem fs = Jimfs.newFileSystem(com.google.common.jimfs.Configuration.unix())) {
                    Path jimfsDirectory = fs.getPath(directoryPath);
                    Files.createDirectory(jimfsDirectory);
                } catch (IOException e) {
                    throw new HttpException(e);
                }
            }

            File cacheDirectory = new File(directoryPath);
            long maxSize = Long.parseLong(config.getOrDefault(OKHTTP_CACHE_MAX_SIZE, DEFAULT_MAX_CACHE_ENTRIES).toString());
            Cache cache = new Cache(cacheDirectory, maxSize, FileSystem.SYSTEM);
            httpClientBuilder.cache(cache);

            DiskSpaceMetrics diskSpaceMetrics = new DiskSpaceMetrics(cacheDirectory);
            diskSpaceMetrics.bindTo(meterRegistry);
        }
    }

    @Override
    public Request buildRequest(HttpRequest httpRequest) {
        Request.Builder builder = new Request.Builder();

        //url
        String url = httpRequest.getUrl();
        HttpUrl okHttpUrl = HttpUrl.parse(url);
        Preconditions.checkNotNull(okHttpUrl, "url cannot be null");
        builder.url(okHttpUrl);

        //headers
        Headers.Builder okHeadersBuilder = new Headers.Builder();
        Map<String, List<String>> headers = httpRequest.getHeaders();
        headers.forEach((key, values) -> {
            for (String value : values) {
                okHeadersBuilder.add(key, value);
            }
        });
        Headers okHeaders = okHeadersBuilder.build();
        builder.headers(okHeaders);
        //Content-Type
        List<String> contentType = headers.get("Content-Type");
        String firstContentType = null;
        if (contentType != null && !contentType.isEmpty()) {
            firstContentType = contentType.get(0);
        }
        RequestBody requestBody = null;
        String method = httpRequest.getMethod();
        if (HttpMethod.permitsRequestBody(method)) {
            if (HttpRequest.BodyType.STRING.equals(httpRequest.getBodyType())) {
                //use the contentType set in HttpRequest. if not set, use application/json
                requestBody = RequestBody.create(httpRequest.getBodyAsString(), MediaType.parse(Optional.ofNullable(firstContentType).orElse("application/json")));
            } else if (HttpRequest.BodyType.BYTE_ARRAY.equals(httpRequest.getBodyType())) {
                String encoded = Base64.getEncoder().encodeToString(httpRequest.getBodyAsByteArray());
                requestBody = RequestBody.create(encoded, MediaType.parse(Optional.ofNullable(firstContentType).orElse("application/octet-stream")));
            } else if (HttpRequest.BodyType.FORM.equals(httpRequest.getBodyType())) {
                FormBody.Builder formBuilder = new FormBody.Builder();
                Map<String, String> multiparts = Maps.newHashMap();
                for (Map.Entry<String, String> entry : multiparts.entrySet()) {
                    formBuilder.add(entry.getKey(), entry.getValue());
                }
                requestBody = formBuilder.build();
            } else {
                //TODO handle multipart
                //HttpRequest.BodyType = MULTIPART
                List<byte[]> bodyAsMultipart = httpRequest.getBodyAsMultipart();
            }
        } else if (httpRequest.getBodyAsString() != null && !httpRequest.getBodyAsString().isBlank()) {
            LOGGER.warn("Http Request with '{}' method does not permit a body. the provided body has been removed. please use another method to use one", method);
        }
        builder.method(method, requestBody);
        return builder.build();
    }

    @Override
    public HttpResponse buildResponse(Response response) {
        HttpResponse httpResponse;
        try {
            Protocol protocol = response.protocol();
            LOGGER.debug("protocol: '{}'", protocol);
            LOGGER.debug("cache-control: '{}'", response.cacheControl());
            LOGGER.debug("handshake: '{}'", response.handshake());
            LOGGER.debug("challenges: '{}'", response.challenges());
            httpResponse = new HttpResponse(response.code(), response.message());
            if (response.body() != null) {
                httpResponse.setResponseBody(response.body().string());
            }
            if (protocol != null) {
                httpResponse.setProtocol(protocol.name());
            }
            Headers headers = response.headers();
            Iterator<Pair<String, String>> iterator = headers.iterator();
            Map<String, List<String>> responseHeaders = Maps.newHashMap();
            while (iterator.hasNext()) {
                Pair<String, String> header = iterator.next();
                responseHeaders.put(header.getFirst(), Lists.newArrayList(header.getSecond()));
            }
            httpResponse.setResponseHeaders(responseHeaders);
        } catch (IOException e) {
            throw new HttpException(e);
        }
        return httpResponse;
    }

    @Override
    public CompletableFuture<Response> nativeCall(Request request) {
        CompletableFuture<Response> cf = new CompletableFuture<>();
        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                cf.completeExceptionally(e);
            }

            @Override
            public void onResponse(Call call, Response response) {
                cf.complete(response);
            }
        });
        return cf;
    }

    /**
     * for tests only.
     *
     * @return {@link okhttp3.OkHttpClient}
     */
    protected okhttp3.OkHttpClient getInternalClient() {
        return client;
    }
}
