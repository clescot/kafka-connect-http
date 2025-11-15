package io.github.clescot.kafka.connect.http.client.okhttp;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.jimfs.Jimfs;
import io.github.clescot.kafka.connect.http.client.HttpClientFactory;
import io.github.clescot.kafka.connect.http.client.okhttp.authentication.*;
import io.github.clescot.kafka.connect.http.client.okhttp.event.AdvancedEventListenerFactory;
import io.github.clescot.kafka.connect.http.client.okhttp.interceptor.InetAddressInterceptor;
import io.github.clescot.kafka.connect.http.client.okhttp.interceptor.LoggingInterceptor;
import io.github.clescot.kafka.connect.http.client.okhttp.interceptor.SSLHandshakeInterceptor;
import io.micrometer.core.instrument.binder.system.DiskSpaceMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import okhttp3.*;
import okhttp3.dnsoverhttps.DnsOverHttps;
import org.jetbrains.annotations.NotNull;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static io.github.clescot.kafka.connect.Configuration.DEFAULT_CONFIGURATION_ID;
import static io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition.*;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.*;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.FALSE;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.TRUE;

public class OkHttpClientFactory implements HttpClientFactory<OkHttpClient,Request, Response> {

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
    public static final String METER_REGISTRY_MUST_NOT_BE_NULL = "MeterRegistry must not be null.";
    private @NotNull TrustManagerFactory trustManagerFactory;
    private static ConnectionPool sharedConnectionPool;

    @Override
    public OkHttpClient build(Map<String, String> config,
                                               ExecutorService executorService,
                                               Random random,
                                               Proxy proxy,
                                               ProxySelector proxySelector,
                                               CompositeMeterRegistry meterRegistry) {
        Preconditions.checkNotNull(random, "Random must not be null.");
        Preconditions.checkNotNull(meterRegistry, METER_REGISTRY_MUST_NOT_BE_NULL);
        okhttp3.OkHttpClient internalOkHttpClient = buildOkHttpClient(config, executorService, random, proxy, proxySelector, meterRegistry);
        OkHttpClient okHttpClient = new OkHttpClient(config, internalOkHttpClient,random);
        okHttpClient.setTrustManagerFactory(trustManagerFactory);
        return okHttpClient;
    }

    private okhttp3.OkHttpClient buildOkHttpClient(Map<String, String> config,
                                                              ExecutorService executorService,
                                                              Random random,
                                                              Proxy proxy,
                                                              ProxySelector proxySelector,
                                                   CompositeMeterRegistry meterRegistry) {
        Preconditions.checkNotNull(random, "Random must not be null.");
        Preconditions.checkNotNull(meterRegistry, METER_REGISTRY_MUST_NOT_BE_NULL);
        okhttp3.OkHttpClient.Builder httpClientBuilder = new okhttp3.OkHttpClient.Builder();

        if (executorService != null) {
            Dispatcher dispatcher = new Dispatcher(executorService);
            if(config.containsKey(OKHTTP_DISPATCHER_MAX_REQUESTS)){
                dispatcher.setMaxRequests(Integer.parseInt(config.get(OKHTTP_DISPATCHER_MAX_REQUESTS)));
            }

            if(config.containsKey(OKHTTP_DISPATCHER_MAX_REQUESTS_PER_HOST)){
                dispatcher.setMaxRequestsPerHost(Integer.parseInt(config.get(OKHTTP_DISPATCHER_MAX_REQUESTS_PER_HOST)));
            }
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
        configureSSL(config, httpClientBuilder, SecureRandom.class.isAssignableFrom(random.getClass())?(SecureRandom) random:null);

        configureConnection(config, httpClientBuilder);

        //cache
        configureCache(config, httpClientBuilder,meterRegistry);



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

        return httpClientBuilder.build();
    }

    private void configureDnsOverHttps(Map<String, String> config, okhttp3.OkHttpClient.Builder httpClientBuilder) {
        if (config.containsKey(OKHTTP_DOH_ACTIVATE)&&Boolean.parseBoolean(config.get(OKHTTP_DOH_ACTIVATE))) {
            List<InetAddress> bootstrapDnsHosts = null;
            if(config.containsKey(OKHTTP_DOH_BOOTSTRAP_DNS_HOSTS)){
                bootstrapDnsHosts = (Arrays.asList(config.get(OKHTTP_DOH_BOOTSTRAP_DNS_HOSTS).split(",")))
                        .stream()
                        .map(host->{
                            try {
                                return InetAddress.getByName(host);
                            } catch (UnknownHostException e) {
                                throw new IllegalArgumentException(e);
                            }
                        })
                        .toList();
            }else {
                LOGGER.debug("no bootstrap DNS set. use system DNS");
            }
            boolean includeipv6 = true;
            if(config.containsKey(OKHTTP_DOH_INCLUDE_IPV6)){
                includeipv6 = Boolean.parseBoolean(config.get(OKHTTP_DOH_INCLUDE_IPV6));
            }
            boolean usePostMethod = false;
            if(config.containsKey(OKHTTP_DOH_USE_POST_METHOD)){
                usePostMethod = Boolean.parseBoolean(config.get(OKHTTP_DOH_USE_POST_METHOD));
            }
            boolean resolvePrivateAddresses = false;
            if(config.containsKey(OKHTTP_DOH_RESOLVE_PRIVATE_ADDRESSES)){
                resolvePrivateAddresses = Boolean.parseBoolean(config.get(OKHTTP_DOH_RESOLVE_PRIVATE_ADDRESSES));
            }

            boolean resolvePublicAddresses = true;
            if(config.containsKey(OKHTTP_DOH_RESOLVE_PUBLIC_ADDRESSES)){
                resolvePublicAddresses = Boolean.parseBoolean(config.get(OKHTTP_DOH_RESOLVE_PUBLIC_ADDRESSES));
            }
            HttpUrl url;
            if(!config.containsKey(OKHTTP_DOH_URL)) {
                throw new IllegalStateException("DNS Over HTTP (DoH) is activated but DoH's URL is not set.");
            }else{
                url = HttpUrl.parse(config.get(OKHTTP_DOH_URL));
            }
            Preconditions.checkNotNull(url, "DoH URL is null.");
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

    private static void configureEvents(Map<String, String> config, CompositeMeterRegistry meterRegistry, okhttp3.OkHttpClient.Builder httpClientBuilder) {
        Preconditions.checkNotNull(meterRegistry, METER_REGISTRY_MUST_NOT_BE_NULL);
        if (!meterRegistry.getRegistries().isEmpty()) {
            List<String> tags = Lists.newArrayList();
            tags.add(HttpClientFactory.CONFIGURATION_ID);
            tags.add(config.get(HttpClientFactory.CONFIGURATION_ID) != null ? config.get(HttpClientFactory.CONFIGURATION_ID) : DEFAULT_CONFIGURATION_ID);
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

            boolean includeLegacyHostTag = Boolean.parseBoolean(config.getOrDefault(METER_REGISTRY_TAG_INCLUDE_LEGACY_HOST, FALSE));
            boolean includeUrlPath = Boolean.parseBoolean(config.getOrDefault(METER_REGISTRY_TAG_INCLUDE_URL_PATH, FALSE));
            httpClientBuilder.eventListenerFactory(new AdvancedEventListenerFactory(meterRegistry, includeLegacyHostTag, includeUrlPath
                    , tags.toArray(new String[0])
            ));
        }
    }

    private static void configureInterceptors(Map<String, String> config,okhttp3.OkHttpClient.Builder httpClientBuilder) {
        boolean activateLoggingInterceptor = Boolean.parseBoolean(config.getOrDefault(CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_LOGGING_ACTIVATE, TRUE));
        if (activateLoggingInterceptor) {
            httpClientBuilder.addNetworkInterceptor(new LoggingInterceptor());
        }
        boolean activateInetAddressInterceptor = Boolean.parseBoolean(config.getOrDefault(CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_INET_ADDRESS_ACTIVATE, FALSE));
        if (activateInetAddressInterceptor) {
            httpClientBuilder.addNetworkInterceptor(new InetAddressInterceptor());
        }
        boolean activateSslHandshakeInterceptor = Boolean.parseBoolean(config.getOrDefault(CONFIG_DEFAULT_OKHTTP_INTERCEPTOR_SSL_HANDSHAKE_ACTIVATE, FALSE));
        if (activateSslHandshakeInterceptor) {
            httpClientBuilder.addNetworkInterceptor(new SSLHandshakeInterceptor());
        }
    }

    private void configureProtocols(Map<String, String> config, okhttp3.OkHttpClient.Builder httpClientBuilder) {
        if (config.containsKey(OKHTTP_PROTOCOLS)) {
            String protocolNames = config.get(OKHTTP_PROTOCOLS);
            List<Protocol> protocols = Lists.newArrayList();
            List<String> strings = Lists.newArrayList(protocolNames.split(PROTOCOL_SEPARATOR));
            for (String protocolName : strings) {
                Protocol protocol = Protocol.valueOf(protocolName);
                protocols.add(protocol);
            }
            httpClientBuilder.protocols(protocols);
        }
    }

    private void configureConnection(Map<String, String> config, okhttp3.OkHttpClient.Builder httpClientBuilder) {
        //call timeout
        if (config.containsKey(OKHTTP_CALL_TIMEOUT)) {
            int callTimeout = Integer.parseInt(config.get(OKHTTP_CALL_TIMEOUT));
            httpClientBuilder.callTimeout(callTimeout, TimeUnit.MILLISECONDS);
        }

        //connect timeout
        if (config.containsKey(OKHTTP_CONNECT_TIMEOUT)) {
            int connectTimeout = Integer.parseInt(config.get(OKHTTP_CONNECT_TIMEOUT));
            httpClientBuilder.connectTimeout(connectTimeout, TimeUnit.MILLISECONDS);
        }

        //read timeout
        if (config.containsKey(OKHTTP_READ_TIMEOUT)) {
            int readTimeout = Integer.parseInt(config.get(OKHTTP_READ_TIMEOUT));
            httpClientBuilder.readTimeout(readTimeout, TimeUnit.MILLISECONDS);
        }

        //write timeout
        if (config.containsKey(OKHTTP_WRITE_TIMEOUT)) {
            int writeTimeout = Integer.parseInt(config.get(OKHTTP_WRITE_TIMEOUT));
            httpClientBuilder.writeTimeout(writeTimeout, TimeUnit.MILLISECONDS);
        }

        //follow redirects
        if (config.containsKey(OKHTTP_FOLLOW_REDIRECT)) {
            httpClientBuilder.followRedirects(Boolean.parseBoolean(config.get(OKHTTP_FOLLOW_REDIRECT)));
        }

        //follow https redirects
        if (config.containsKey(OKHTTP_FOLLOW_SSL_REDIRECT)) {
            httpClientBuilder.followSslRedirects(Boolean.parseBoolean(config.get(OKHTTP_FOLLOW_SSL_REDIRECT)));
        }

        //retry on connection failure
        if (config.containsKey(OKHTTP_RETRY_ON_CONNECTION_FAILURE)) {
            httpClientBuilder.retryOnConnectionFailure(Boolean.parseBoolean(config.get(OKHTTP_RETRY_ON_CONNECTION_FAILURE)));
        }

    }

    @SuppressWarnings("java:S5527")
    private void configureSSL(Map<String,String> config, okhttp3.OkHttpClient.Builder httpClientBuilder, SecureRandom random) {
        //KeyManager/trustManager/SSLSocketFactory
        Optional<KeyManagerFactory> keyManagerFactoryOption = getKeyManagerFactory(config);
        Optional<TrustManagerFactory> trustManagerFactoryOption = HttpClientFactory.buildTrustManagerFactory(config);
        trustManagerFactoryOption.ifPresent(managerFactory -> this.trustManagerFactory = managerFactory);

        if (keyManagerFactoryOption.isPresent() || trustManagerFactoryOption.isPresent()) {
            if(random==null){
                random = new SecureRandom();
            }
            //TODO SSLFactory protocol parameter
            SSLSocketFactory ssl = HttpClientFactory.getSSLSocketFactory(keyManagerFactoryOption.orElse(null), trustManagerFactoryOption.orElse(null), "SSL",random);
            if (trustManagerFactoryOption.isPresent()) {
                TrustManager[] trustManagers = trustManagerFactoryOption.get().getTrustManagers();
                if (trustManagers.length > 0) {
                    httpClientBuilder.sslSocketFactory(ssl, (X509TrustManager) trustManagers[0]);
                }
            }
            if (config.containsKey(OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION) && Boolean.parseBoolean(config.get(OKHTTP_SSL_SKIP_HOSTNAME_VERIFICATION))) {
                httpClientBuilder.hostnameVerifier((hostname, session) -> true);
            }
        }
    }

    private void configureConnectionPool(Map<String, String> config, okhttp3.OkHttpClient.Builder httpClientBuilder) {
        String connectionPoolScope = config.getOrDefault(OKHTTP_CONNECTION_POOL_SCOPE, "instance");
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
    private static ConnectionPool buildConnectionPool(Map<String, String> config, ConnectionPool connectionPool) {
        int maxIdleConnections = Integer.parseInt(config.getOrDefault(OKHTTP_CONNECTION_POOL_MAX_IDLE_CONNECTIONS, "0"));
        long keepAliveDuration = Long.parseLong(config.getOrDefault(OKHTTP_CONNECTION_POOL_KEEP_ALIVE_DURATION, "0"));
        if (maxIdleConnections > 0 && keepAliveDuration > 0) {
            connectionPool = new ConnectionPool(maxIdleConnections, keepAliveDuration, TimeUnit.MILLISECONDS);
        }
        return connectionPool;
    }



    private void configureCache(Map<String, String> config, okhttp3.OkHttpClient.Builder httpClientBuilder, CompositeMeterRegistry meterRegistry) {
        if (config.containsKey(OKHTTP_CACHE_ACTIVATE)) {
            String cacheType = config.getOrDefault(OKHTTP_CACHE_TYPE, FILE_CACHE_TYPE);
            String defaultDirectoryPath;
            if (IN_MEMORY_CACHE_TYPE.equalsIgnoreCase(cacheType)) {
                defaultDirectoryPath = DEFAULT_IN_MEMORY_DIRECTORY_CACHE_PATH;
            } else {
                defaultDirectoryPath = DEFAULT_FILE_DIRECTORY_CACHE_PATH;
            }

            String directoryPath = config.getOrDefault(OKHTTP_CACHE_DIRECTORY_PATH, defaultDirectoryPath);

            if (IN_MEMORY_CACHE_TYPE.equalsIgnoreCase(cacheType)) {
                try (java.nio.file.FileSystem fs = Jimfs.newFileSystem(com.google.common.jimfs.Configuration.unix())) {
                    Path jimfsDirectory = fs.getPath(directoryPath);
                    Files.createDirectory(jimfsDirectory);
                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
            }

            File cacheDirectory = new File(directoryPath);
            long maxSize = Long.parseLong(config.getOrDefault(OKHTTP_CACHE_MAX_SIZE, DEFAULT_MAX_CACHE_ENTRIES));
            Cache cache = new Cache(cacheDirectory, maxSize);
            httpClientBuilder.cache(cache);

            DiskSpaceMetrics diskSpaceMetrics = new DiskSpaceMetrics(cacheDirectory);
            diskSpaceMetrics.bindTo(meterRegistry);
        }
    }

    @NotNull
    public TrustManagerFactory getTrustManagerFactory() {
        return trustManagerFactory;
    }

    private static void setSharedConnectionPool(ConnectionPool connectionPool) {
        sharedConnectionPool = connectionPool;
    }

    private static ConnectionPool getSharedConnectionPool() {
        return sharedConnectionPool;
    }

}
