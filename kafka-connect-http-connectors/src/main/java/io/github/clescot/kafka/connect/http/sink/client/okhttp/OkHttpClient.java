package io.github.clescot.kafka.connect.http.sink.client.okhttp;

import com.burgstaller.okhttp.AuthenticationCacheInterceptor;
import com.burgstaller.okhttp.CachingAuthenticatorDecorator;
import com.burgstaller.okhttp.DispatchingAuthenticator;
import com.burgstaller.okhttp.basic.BasicAuthenticator;
import com.burgstaller.okhttp.digest.CachingAuthenticator;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.sink.client.AbstractHttpClient;
import io.github.clescot.kafka.connect.http.sink.client.HttpException;
import kotlin.Pair;
import okhttp3.*;
import okhttp3.internal.http.HttpMethod;
import okhttp3.internal.io.FileSystem;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

public class OkHttpClient extends AbstractHttpClient<Request, Response> {
    private static final String PROTOCOL_SEPARATOR = ",";

    public static final String IN_MEMORY_CACHE_TYPE = "inmemory";
    public static final String DEFAULT_MAX_CACHE_ENTRIES = "10000";
    public static final String FILE_CACHE_TYPE = "file";
    public static final String SHA_1_PRNG = "SHA1PRNG";
    public static final String US_ASCII = "US-ASCII";
    public static final String ISO_8859_1 = "ISO-8859-1";


    private final okhttp3.OkHttpClient client;
    private static final Logger LOGGER = LoggerFactory.getLogger(OkHttpClient.class);

    @SuppressWarnings("java:S5527")
    public OkHttpClient(Map<String, Object> config, ExecutorService executorService) {
        super(config);
        okhttp3.OkHttpClient.Builder httpClientBuilder = new okhttp3.OkHttpClient.Builder();
        if (executorService != null) {
            Dispatcher dispatcher = new Dispatcher(executorService);
            httpClientBuilder.dispatcher(dispatcher);
        }
        configureConnectionPool(config, httpClientBuilder);
        //protocols
        configureProtocols(config, httpClientBuilder);
        configureSSL(config, httpClientBuilder);

        configureConnection(config, httpClientBuilder);

        //cache
        configureCache(config, httpClientBuilder);

        //authentication
        configureAuthentication(config, httpClientBuilder);

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
            int callTimeout = (Integer)config.get(OKHTTP_CALL_TIMEOUT);
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
        if(config.containsKey(OKHTTP_FOLLOW_REDIRECT)){
            httpClientBuilder.followRedirects((Boolean) config.get(OKHTTP_FOLLOW_REDIRECT));
        }

        //follow https redirects
        if(config.containsKey(OKHTTP_FOLLOW_SSL_REDIRECT)){
            httpClientBuilder.followSslRedirects((Boolean) config.get(OKHTTP_FOLLOW_SSL_REDIRECT));
        }
    }

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
        int maxIdleConnections = Integer.parseInt(config.getOrDefault(OKHTTP_CONNECTION_POOL_MAX_IDLE_CONNECTIONS, "0").toString());
        long keepAliveDuration = Long.parseLong(config.getOrDefault(OKHTTP_CONNECTION_POOL_KEEP_ALIVE_DURATION, "0").toString());
        if (maxIdleConnections > 0 && keepAliveDuration > 0) {
            ConnectionPool connectionPool = new ConnectionPool(maxIdleConnections, keepAliveDuration, TimeUnit.MILLISECONDS);
            httpClientBuilder.connectionPool(connectionPool);
        }
    }

    private void configureAuthentication(Map<String, Object> config, okhttp3.OkHttpClient.Builder httpClientBuilder) {
        final Map<String, CachingAuthenticator> authCache = new ConcurrentHashMap<>();

        BasicAuthenticator basicAuthenticator = configureBasicAuthentication(config);

        DigestAuthenticator digestAuthenticator = configureDigestAuthenticator(config);

        // note that all auth schemes should be registered as lowercase!
        DispatchingAuthenticator.Builder authenticatorBuilder = new DispatchingAuthenticator.Builder();
        if (basicAuthenticator != null) {
            authenticatorBuilder = authenticatorBuilder.with("basic", basicAuthenticator);
        }
        if (digestAuthenticator != null) {
            authenticatorBuilder = authenticatorBuilder.with("digest", digestAuthenticator);
        }
        if (basicAuthenticator != null || digestAuthenticator != null) {
            httpClientBuilder.authenticator(new CachingAuthenticatorDecorator(authenticatorBuilder.build(), authCache));
            httpClientBuilder.addInterceptor(new AuthenticationCacheInterceptor(authCache));
            httpClientBuilder.addNetworkInterceptor(new LoggingInterceptor());
        }
    }

    @Nullable
    private DigestAuthenticator configureDigestAuthenticator(Map<String, Object> config) {
        //Digest Authentication
        DigestAuthenticator digestAuthenticator = null;
        if (config.containsKey(HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE) && Boolean.TRUE.equals(config.get(HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE))) {
            String username = (String) config.get(HTTP_CLIENT_AUTHENTICATION_DIGEST_USERNAME);
            String password = (String) config.get(HTTP_CLIENT_AUTHENTICATION_DIGEST_PASSWORD);
            com.burgstaller.okhttp.digest.Credentials credentials = new com.burgstaller.okhttp.digest.Credentials(username, password);
            //digest charset
            String digestCredentialCharset = US_ASCII;
            if (config.containsKey(HTTP_CLIENT_AUTHENTICATION_DIGEST_CHARSET)) {
                digestCredentialCharset = String.valueOf(config.get(HTTP_CLIENT_AUTHENTICATION_DIGEST_CHARSET));
            }
            Charset digestCharset = Charset.forName(digestCredentialCharset);

            SecureRandom random;
            String rngAlgorithm = SHA_1_PRNG;
            if (config.containsKey(HTTP_CLIENT_AUTHENTICATION_DIGEST_SECURE_RANDOM_PRNG_ALGORITHM)) {
                rngAlgorithm = (String) config.get(HTTP_CLIENT_AUTHENTICATION_DIGEST_SECURE_RANDOM_PRNG_ALGORITHM);
            }
            try {
                random = SecureRandom.getInstance(rngAlgorithm);
            } catch (NoSuchAlgorithmException e) {
                throw new HttpException(e);
            }
            digestAuthenticator = new DigestAuthenticator(credentials, digestCharset, random);

        }
        return digestAuthenticator;
    }

    @Nullable
    private BasicAuthenticator configureBasicAuthentication(Map<String, Object> config) {
        //Basic authentication
        BasicAuthenticator basicAuthenticator = null;
        if (config.containsKey(HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE) && Boolean.TRUE.equals(config.get(HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE))) {
            String username = (String) config.get(HTTP_CLIENT_AUTHENTICATION_BASIC_USERNAME);
            String password = (String) config.get(HTTP_CLIENT_AUTHENTICATION_BASIC_PASSWORD);
            com.burgstaller.okhttp.digest.Credentials credentials = new com.burgstaller.okhttp.digest.Credentials(username, password);


            //basic charset
            String basicCredentialCharset = ISO_8859_1;
            if (config.containsKey(HTTP_CLIENT_AUTHENTICATION_BASIC_CHARSET)) {
                basicCredentialCharset = String.valueOf(config.get(HTTP_CLIENT_AUTHENTICATION_BASIC_CHARSET));
            }
            Charset basicCharset = Charset.forName(basicCredentialCharset);
            basicAuthenticator = new BasicAuthenticator(credentials, basicCharset);
        }
        return basicAuthenticator;
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
                try (java.nio.file.FileSystem fs = Jimfs.newFileSystem(Configuration.unix())) {
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
        }
    }

    @Override
    public Request buildRequest(HttpRequest httpRequest) {
        Request.Builder builder = new Request.Builder();

        //url
        String url = httpRequest.getUrl();
        HttpUrl okHttpUrl = HttpUrl.parse(url);
        Preconditions.checkNotNull(okHttpUrl,"url cannot be null");
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
                Map<String, String> multiparts = null;
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

}
