package io.github.clescot.kafka.connect.http.client.okhttp;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.jimfs.Jimfs;
import io.github.clescot.kafka.connect.http.client.AbstractHttpClient;
import io.github.clescot.kafka.connect.http.client.Configuration;
import io.github.clescot.kafka.connect.http.client.HttpException;
import io.github.clescot.kafka.connect.http.client.okhttp.authentication.*;
import io.github.clescot.kafka.connect.http.client.okhttp.event.AdvancedEventListenerFactory;
import io.github.clescot.kafka.connect.http.client.okhttp.interceptor.InetAddressInterceptor;
import io.github.clescot.kafka.connect.http.client.okhttp.interceptor.LoggingInterceptor;
import io.github.clescot.kafka.connect.http.client.okhttp.interceptor.SSLHandshakeInterceptor;
import io.github.clescot.kafka.connect.http.core.HttpPart;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.core.HttpResponseBuilder;
import io.micrometer.core.instrument.binder.system.DiskSpaceMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import kotlin.Pair;
import okhttp3.*;
import okhttp3.dnsoverhttps.DnsOverHttps;
import okhttp3.internal.http.HttpMethod;
import okhttp3.internal.io.FileSystem;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.net.ssl.*;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;
import static io.github.clescot.kafka.connect.http.sink.HttpSinkTask.DEFAULT_CONFIGURATION_ID;

public class OkHttpClient extends AbstractHttpClient<Request, Response> {
    private static final String PROTOCOL_SEPARATOR = ",";

    public static final String IN_MEMORY_CACHE_TYPE = "inmemory";
    public static final String DEFAULT_MAX_CACHE_ENTRIES = "10000";
    public static final String FILE_CACHE_TYPE = "file";
    public static final String CONNECTOR_NAME = "connector.name";
    public static final String CONNECTOR_TASK = "connector.task";
    @SuppressWarnings("java:S1075")
    public static final String DEFAULT_IN_MEMORY_DIRECTORY_CACHE_PATH = "/kafka-connect-http-cache";
    @SuppressWarnings("java:S1075")
    public static final String DEFAULT_FILE_DIRECTORY_CACHE_PATH = "/tmp/kafka-connect-http-cache";


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
        configureSSL(config, httpClientBuilder,SecureRandom.class.isAssignableFrom(random.getClass())?(SecureRandom) random:null);

        configureConnection(config, httpClientBuilder);

        //cache
        configureCache(config, httpClientBuilder);



        //interceptors
        configureInterceptors(config, httpClientBuilder);

        //events
        configureEvents(config, meterRegistry, httpClientBuilder);

        //authentication
        AuthenticationConfigurer basicAuthenticationConfigurer = new BasicAuthenticationConfigurer();
        AuthenticationConfigurer digestAuthenticationConfigurer = new DigestAuthenticationConfigurer(random);
        AuthenticationConfigurer oAuth2ClientCredentialsFlowConfigurer = new OAuth2ClientCredentialsFlowConfigurer(httpClientBuilder.build());
        List<AuthenticationConfigurer> authenticatorConfigurers = Lists.newArrayList(
                basicAuthenticationConfigurer,
                digestAuthenticationConfigurer,
                oAuth2ClientCredentialsFlowConfigurer
        );
        AuthenticationsConfigurer authenticationsConfigurer = new AuthenticationsConfigurer(authenticatorConfigurers);
        authenticationsConfigurer.configure(config, httpClientBuilder);



        //dns
        configureDnsOverHttps(config, httpClientBuilder);

        client = httpClientBuilder.build();
    }

    private void configureDnsOverHttps(Map<String, Object> config, okhttp3.OkHttpClient.Builder httpClientBuilder) {
        if (config.containsKey(OKHTTP_DOH_ACTIVATE)&&Boolean.parseBoolean((String) config.get(OKHTTP_DOH_ACTIVATE))) {
            List<InetAddress> bootstrapDnsHosts = null;
            if(config.containsKey(OKHTTP_DOH_BOOTSTRAP_DNS_HOSTS)){
                bootstrapDnsHosts = ((List<String>)config.get(OKHTTP_DOH_BOOTSTRAP_DNS_HOSTS))
                        .stream()
                        .map(host->{
                            try {
                                return InetAddress.getByName(host);
                            } catch (UnknownHostException e) {
                                throw new IllegalArgumentException(e);
                            }
                        })
                        .collect(Collectors.toList());
            }else {
                LOGGER.debug("no bootstrap DNS set. use system DNS");
            };
            boolean includeipv6 = true;
            if(config.containsKey(OKHTTP_DOH_INCLUDE_IPV6)){
                includeipv6 = Boolean.parseBoolean((String) config.get(OKHTTP_DOH_INCLUDE_IPV6));
            }
            boolean usePostMethod = false;
            if(config.containsKey(OKHTTP_DOH_USE_POST_METHOD)){
                usePostMethod = Boolean.parseBoolean((String) config.get(OKHTTP_DOH_USE_POST_METHOD));
            }
            boolean resolvePrivateAddresses = false;
            if(config.containsKey(OKHTTP_DOH_RESOLVE_PRIVATE_ADDRESSES)){
                resolvePrivateAddresses = Boolean.parseBoolean((String) config.get(OKHTTP_DOH_RESOLVE_PRIVATE_ADDRESSES));
            }

            boolean resolvePublicAddresses = true;
            if(config.containsKey(OKHTTP_DOH_RESOLVE_PUBLIC_ADDRESSES)){
                resolvePublicAddresses = Boolean.parseBoolean((String) config.get(OKHTTP_DOH_RESOLVE_PUBLIC_ADDRESSES));
            }
            HttpUrl url;
            if(!config.containsKey(OKHTTP_DOH_URL)) {
                throw new IllegalStateException("DNS Over HTTP (DoH) is activated but DoH's URL is not set.");
            }else{
                url = HttpUrl.parse((String) config.get(OKHTTP_DOH_URL));
            }

            okhttp3.OkHttpClient bootstrapClient = httpClientBuilder.build();
            DnsOverHttps dnsOverHttps = new DnsOverHttps.Builder().client(bootstrapClient)
                    .bootstrapDnsHosts(bootstrapDnsHosts)
                    .includeIPv6(includeipv6)
                    .post(usePostMethod)
                    .resolvePrivateAddresses(resolvePrivateAddresses)
                    .resolvePublicAddresses(resolvePublicAddresses)
                    .url(url)
                    .build();
            httpClientBuilder.dns(dnsOverHttps);
        }
    }

    private static void configureEvents(Map<String, Object> config, CompositeMeterRegistry meterRegistry, okhttp3.OkHttpClient.Builder httpClientBuilder) {
        if (!meterRegistry.getRegistries().isEmpty()) {
            List<String> tags = Lists.newArrayList();
            tags.add(Configuration.CONFIGURATION_ID);
            tags.add(config.get(Configuration.CONFIGURATION_ID) != null ? (String) config.get(Configuration.CONFIGURATION_ID) : DEFAULT_CONFIGURATION_ID);
            String connectorName = MDC.get(CONNECTOR_NAME);
            if (connectorName != null) {
                tags.add(CONNECTOR_NAME);
                tags.add(connectorName);
            }

            String connectorTask = MDC.get(CONNECTOR_TASK);
            if (connectorTask != null) {
                tags.add(CONNECTOR_TASK);
                tags.add(connectorTask);
            }

            boolean includeLegacyHostTag = Boolean.parseBoolean((String) config.getOrDefault(METER_REGISTRY_TAG_INCLUDE_LEGACY_HOST, FALSE));
            boolean includeUrlPath = Boolean.parseBoolean((String) config.getOrDefault(METER_REGISTRY_TAG_INCLUDE_URL_PATH, FALSE));
            httpClientBuilder.eventListenerFactory(new AdvancedEventListenerFactory(meterRegistry, includeLegacyHostTag, includeUrlPath
                    , tags.toArray(new String[0])
            ));
        }
    }

    private static void configureInterceptors(Map<String, Object> config,okhttp3.OkHttpClient.Builder httpClientBuilder) {
        boolean activateLoggingInterceptor = Boolean.parseBoolean((String) config.getOrDefault(CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_LOGGING_ACTIVATE, TRUE));
        if (activateLoggingInterceptor) {
            httpClientBuilder.addNetworkInterceptor(new LoggingInterceptor());
        }
        boolean activateInetAddressInterceptor = Boolean.parseBoolean((String) config.getOrDefault(CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_INET_ADDRESS_ACTIVATE, FALSE));
        if (activateInetAddressInterceptor) {
            httpClientBuilder.addNetworkInterceptor(new InetAddressInterceptor());
        }
        boolean activateSslHandshakeInterceptor = Boolean.parseBoolean((String) config.getOrDefault(CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_SSL_HANDSHAKE_ACTIVATE, FALSE));
        if (activateSslHandshakeInterceptor) {
            httpClientBuilder.addNetworkInterceptor(new SSLHandshakeInterceptor());
        }
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

        //retry on connection failure
        if (config.containsKey(OKHTTP_RETRY_ON_CONNECTION_FAILURE)) {
            httpClientBuilder.retryOnConnectionFailure((Boolean) config.get(OKHTTP_RETRY_ON_CONNECTION_FAILURE));
        }

    }

    @SuppressWarnings("java:S5527")
    private void configureSSL(Map<String, Object> config, okhttp3.OkHttpClient.Builder httpClientBuilder, SecureRandom random) {
        //KeyManager/trustManager/SSLSocketFactory
        Optional<KeyManagerFactory> keyManagerFactoryOption = getKeyManagerFactory();
        Optional<TrustManagerFactory> trustManagerFactoryOption = buildTrustManagerFactory();
        trustManagerFactoryOption.ifPresent(managerFactory -> this.trustManagerFactory = managerFactory);

        if (keyManagerFactoryOption.isPresent() || trustManagerFactoryOption.isPresent()) {
            if(random==null){
                random = new SecureRandom();
            }
            //TODO SSLFactory protocol parameter
            SSLSocketFactory ssl = AbstractHttpClient.getSSLSocketFactory(keyManagerFactoryOption.orElse(null), trustManagerFactoryOption.orElse(null), "SSL",random);
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
                defaultDirectoryPath = DEFAULT_IN_MEMORY_DIRECTORY_CACHE_PATH;
            } else {
                defaultDirectoryPath = DEFAULT_FILE_DIRECTORY_CACHE_PATH;
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
        Map<String, List<String>> headers = httpRequest.getHeaders();
        Headers okHeaders = getHeaders(headers);
        builder.headers(okHeaders);
        //Content-Type
        List<String> contentType = headers.get("Content-Type");
        String firstContentType = null;
        if (contentType != null && !contentType.isEmpty()) {
            firstContentType = contentType.get(0);
        }
        String method = httpRequest.getMethod().name();
        RequestBody requestBody = null;
        if (HttpMethod.permitsRequestBody(method)) {
            switch (httpRequest.getBodyType()){

                case BYTE_ARRAY:{
                    byte[] bodyAsByteArray = httpRequest.getBodyAsByteArray();
                    requestBody = toRequestBody(bodyAsByteArray, firstContentType);
                    //file upload through a FORM is the same as a byte array (content of the file). this case handle also this case, except part of a multipart request
                    break;
                }

                case FORM: {
                    //contentType is  application/x-www-form-urlencoded

                    Map<String, String> entries = httpRequest.getBodyAsForm();
                    FormBody.Builder formBuilder = new FormBody.Builder();
                    for (Map.Entry<String, String> entry : entries.entrySet()) {
                        formBuilder.add(entry.getKey(), entry.getValue());
                    }
                    requestBody = formBuilder.build();
                    break;
                }
                case MULTIPART: {
                    //HttpRequest.BodyType = MULTIPART
                    Map<String, HttpPart> bodyAsMultipart = httpRequest.getParts();
                    String boundary = null;
                    if (firstContentType != null && firstContentType.contains("boundary=")) {
                        List<String> myParts = Lists.newArrayList(firstContentType.split("boundary="));
                        if (myParts.size() == 2) {
                            boundary = myParts.get(1);
                        }
                    }
                    MultipartBody.Builder multipartBuilder = new MultipartBody.Builder(MoreObjects.firstNonNull(boundary, MoreObjects.firstNonNull(boundary,"---")));
                    multipartBuilder.setType(MediaType.parse("multipart/form-data"));
                    for (Map.Entry<String, HttpPart> entry : bodyAsMultipart.entrySet()) {
                        RequestBody partRequestBody;
                        String parameterName = entry.getKey();
                        HttpPart httpPart = entry.getValue();
                        Map<String, List<String>> partHeaders = httpPart.getHeaders();
                        Headers okPartHeaders = getHeaders(filterHeaders(partHeaders));
                        switch (httpPart.getBodyType()) {
                            //HttpPart is <string,optional<file>>
                            case FORM_DATA:
                                Map.Entry<String, File> contentAsFormEntry = httpPart.getContentAsFormEntry();
                                String fileName = contentAsFormEntry.getKey();
                                File file = contentAsFormEntry.getValue();
                                requestBody = RequestBody.create(file, MediaType.parse("application/octet-stream"));
                                multipartBuilder.addFormDataPart(parameterName, fileName, requestBody);
                                break;
                            //HttpPart is <fileUri>
                            case FORM_DATA_AS_REFERENCE:
                                Map.Entry<String, File> formEntry = httpPart.getContentAsFormEntry();
                                File fileAsReference = new File(httpPart.getFileUri());
                                requestBody = RequestBody.create(fileAsReference, MediaType.parse("application/octet-stream"));
                                multipartBuilder.addFormDataPart(parameterName, formEntry.getKey(), requestBody);
                                break;
                            //HttpPart is <string,byte[]>
                            case BYTE_ARRAY:
                                partRequestBody = toRequestBody(httpPart.getContentAsByteArray(), httpPart.getContentType());
                                multipartBuilder.addPart(okPartHeaders,partRequestBody);
                                break;
                            //HttpPart is <string>
                            case STRING:
                            default:
                                String contentAsString = httpPart.getContentAsString();
                                partRequestBody = RequestBody.create(contentAsString, MediaType.parse(httpPart.getContentType()));
                                multipartBuilder.addPart(okPartHeaders,partRequestBody);
                                break;
                        }
                    }
                    requestBody = multipartBuilder.build();
                    break;
                }

                case STRING:
                default: {
                    //use the contentType set in HttpRequest. if not set, use application/json
                    requestBody = RequestBody.create(httpRequest.getBodyAsString(), MediaType.parse(Optional.ofNullable(firstContentType).orElse("application/json")));
                    break;
                }
            }

        } else if (httpRequest.getBodyAsString() != null && !httpRequest.getBodyAsString().isBlank()) {
            LOGGER.warn("Http Request with '{}' method does not permit a body. the provided body has been removed. please use another method to use one", method);
        }
        builder.method(method, requestBody);
        return builder.build();
    }

    private Map<String, List<String>> filterHeaders(Map<String, List<String>> headers) {
        return headers != null ? headers.entrySet().stream()
                .filter(entry -> !entry.getKey().equalsIgnoreCase("Content-Type"))
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue
                        )
                ) : Maps.newHashMap();
    }


    @NotNull
    private static Headers getHeaders(Map<String, List<String>> headers) {
        Headers.Builder okHeadersBuilder = new Headers.Builder();
        headers.forEach((key, values) -> {
            for (String value : values) {
                okHeadersBuilder.add(key, value);
            }
        });
        return okHeadersBuilder.build();
    }

    @NotNull
    private static RequestBody toRequestBody(byte[] bodyAsByteArray, String contentType) {
        RequestBody requestBody;
        String encoded = Base64.getEncoder().encodeToString(bodyAsByteArray);
        //use the contentType set in HttpRequest. if not set, use application/octet-stream
        requestBody = RequestBody.create(encoded, MediaType.parse(Optional.ofNullable(contentType).orElse("application/octet-stream")));
        return requestBody;
    }


    @Override
    public HttpResponse buildResponse(HttpResponseBuilder httpResponseBuilder, Response response) {
        HttpResponse httpResponse;
        try {
            Protocol protocol = response.protocol();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("native response :'{}'", response);
                LOGGER.trace("protocol: '{}',cache-control: '{}',handshake: '{}',challenges: '{}'", protocol, response.cacheControl(), response.handshake(), response.challenges());
            }
            httpResponseBuilder.setStatus(response.code(), response.message());
            //TODO handle more bodyType for HttpResponse
            if (response.body() != null) {
                httpResponseBuilder.setBodyAsString(response.body().string());
            }
            if (protocol != null) {
                httpResponseBuilder.setProtocol(protocol.name());
            }
            Headers headers = response.headers();
            Iterator<Pair<String, String>> iterator = headers.iterator();
            Map<String, List<String>> responseHeaders = Maps.newHashMap();
            while (iterator.hasNext()) {
                Pair<String, String> header = iterator.next();
                responseHeaders.put(header.getFirst(), Lists.newArrayList(header.getSecond()));
            }
            httpResponseBuilder.setHeaders(responseHeaders);
        } catch (IOException e) {
            throw new HttpException(e);
        }
        return httpResponseBuilder.toHttpResponse();
    }

    @Override
    public CompletableFuture<Response> nativeCall(Request request) {
        CompletableFuture<Response> cf = new CompletableFuture<>();
        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(@NotNull Call call, @NotNull IOException e) {
                LOGGER.error("okhttp native call error :{}", ExceptionUtils.getStackTrace(e));
                cf.completeExceptionally(e);
            }

            @Override
            public void onResponse(@NotNull Call call, @NotNull Response response) {
                cf.complete(response);
            }
        });
        return cf;
    }

    @Override
    public String getEngineId() {
        return "okhttp";
    }

    /**
     * @return {@link okhttp3.OkHttpClient}
     */
    @Override
    public okhttp3.OkHttpClient getInternalClient() {
        return client;
    }
}
