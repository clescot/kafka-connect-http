package io.github.clescot.kafka.connect.http.client;

import dev.failsafe.RateLimiter;
import io.github.clescot.kafka.connect.http.core.HttpExchange;

import javax.annotation.Nullable;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

public abstract class AbstractHttpClient<Req, Res> implements HttpClient<Req, Res> {

    private static final String DEFAULT_SSL_PROTOCOL = "SSL";
    protected Map<String, Object> config;
    private Optional<RateLimiter<HttpExchange>> rateLimiter = Optional.empty();

    protected AbstractHttpClient(Map<String, Object> config) {
        this.config = config;
    }

    protected Optional<TrustManagerFactory> getTrustManagerFactory() {
        if (config.containsKey(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PATH)
                && config.containsKey(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD)) {

            Optional<TrustManagerFactory> trustManagerFactory = Optional.ofNullable(
                    HttpClient.getTrustManagerFactory(config));
            if (trustManagerFactory.isPresent()) {
                return Optional.of(trustManagerFactory.get());
            }
        }
        return Optional.empty();
    }

    protected Optional<KeyManagerFactory> getKeyManagerFactory() {
        if (config.containsKey(CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PATH)
                && config.containsKey(CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PASSWORD)) {

            Optional<KeyManagerFactory> keyManagerFactory = Optional.ofNullable(
                    getKeyManagerFactory(
                            config.get(CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PATH).toString(),
                            config.get(CONFIG_HTTP_CLIENT_SSL_KEYSTORE_PASSWORD).toString().toCharArray(),
                            config.get(CONFIG_HTTP_CLIENT_SSL_KEYSTORE_TYPE).toString(),
                            config.get(CONFIG_HTTP_CLIENT_SSL_KEYSTORE_ALGORITHM).toString()));
            if (keyManagerFactory.isPresent()) {
                return Optional.of(keyManagerFactory.get());
            }
        }
        return Optional.empty();
    }


    private static KeyManagerFactory getKeyManagerFactory(String keyStorePath,
                                                  char[] password,
                                                  @Nullable String keystoreType,
                                                  @Nullable String algorithm) {
        KeyManagerFactory keyManagerFactory;
        KeyStore keyStore;
        try {
            String finalAlgorithm = Optional.ofNullable(algorithm).orElse(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory = KeyManagerFactory.getInstance(finalAlgorithm);
            String finalKeystoreType = Optional.ofNullable(keystoreType).orElse(KeyStore.getDefaultType());
            keyStore = KeyStore.getInstance(finalKeystoreType);
        } catch (NoSuchAlgorithmException | KeyStoreException e) {
            throw new HttpException(e);
        }

        Path path = Path.of(keyStorePath);
        File file = path.toFile();
        try (InputStream inputStream = new FileInputStream(file)) {
            keyStore.load(inputStream, password);
            keyManagerFactory.init(keyStore,password);
        } catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException |
                 UnrecoverableKeyException e) {
            throw new HttpException(e);
        }
        return keyManagerFactory;
    }


    protected static SSLSocketFactory getSSLSocketFactory(@Nullable KeyManagerFactory keyManagerFactory,
                                                          @Nullable TrustManagerFactory trustManagerFactory,
                                                @Nullable String protocol){
        try {
            SSLContext sslContext = SSLContext.getInstance(Optional.ofNullable(protocol).orElse(DEFAULT_SSL_PROTOCOL));
            SecureRandom random = new SecureRandom();
            sslContext.init(keyManagerFactory!=null?keyManagerFactory.getKeyManagers():null,trustManagerFactory!=null?trustManagerFactory.getTrustManagers():null, random);
            return sslContext.getSocketFactory();
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new HttpException(e);
        }
    }

    @Override
    public CompletableFuture<Res> call(Req request){
        try {
            Optional<RateLimiter<HttpExchange>> limiter = getRateLimiter();
            if (limiter.isPresent()) {
                limiter.get().acquirePermits(HttpClient.ONE_HTTP_REQUEST);
                LOGGER.debug("permits acquired request:'{}'", request);
            }
            return nativeCall(request);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HttpException(e.getMessage());
        }
    }

    @Override
    public void setRateLimiter(RateLimiter<HttpExchange> rateLimiter) {
        this.rateLimiter = Optional.ofNullable(rateLimiter);
    }

    @Override
    public Optional<RateLimiter<HttpExchange>> getRateLimiter() {
        return this.rateLimiter;
    }

}