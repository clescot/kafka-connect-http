package io.github.clescot.kafka.connect.http.client;

import com.google.common.base.Preconditions;
import io.github.clescot.kafka.connect.http.client.proxy.ProxySelectorFactory;
import io.github.clescot.kafka.connect.http.client.ssl.AlwaysTrustManagerFactory;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import jakarta.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.nio.file.Path;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

public interface HttpClientFactory<C extends HttpClient<R,S>,R,S> {

    Logger LOGGER = LoggerFactory.getLogger(HttpClientFactory.class);
    String DEFAULT_SSL_PROTOCOL = "SSL";
    String IS_NOT_SET = " is not set";

    C build(Map<String, Object> config,
                              ExecutorService executorService,
                              Random random,
                              Proxy proxy,
                              ProxySelector proxySelector, CompositeMeterRegistry meterRegistry);

    default C buildHttpClient(Map<String, Object> config,
                                                  ExecutorService executorService,
                                                  CompositeMeterRegistry meterRegistry, Random random) {

        //get proxy
        Proxy proxy = null;
        if (config.containsKey(PROXY_HTTP_CLIENT_HOSTNAME)) {
            String proxyHostName = (String) config.get(PROXY_HTTP_CLIENT_HOSTNAME);
            int proxyPort = (Integer) config.get(PROXY_HTTP_CLIENT_PORT);
            SocketAddress socketAddress = new InetSocketAddress(proxyHostName, proxyPort);
            String proxyTypeLabel = (String) Optional.ofNullable(config.get(PROXY_HTTP_CLIENT_TYPE)).orElse("HTTP");
            Proxy.Type proxyType = Proxy.Type.valueOf(proxyTypeLabel);
            proxy = new Proxy(proxyType, socketAddress);
        }

        ProxySelectorFactory proxySelectorFactory = new ProxySelectorFactory();
        ProxySelector proxySelector = null;
        if (config.get(PROXY_SELECTOR_HTTP_CLIENT_0_HOSTNAME) != null) {
            proxySelector = proxySelectorFactory.build(config, random);
        }

        return build(config, executorService, random, proxy, proxySelector, meterRegistry);
    }


    default Optional<KeyManagerFactory> getKeyManagerFactory(Map<String, Object> config) {
        if (config.containsKey(HTTP_CLIENT_SSL_KEYSTORE_PATH)
                && config.containsKey(HTTP_CLIENT_SSL_KEYSTORE_PASSWORD)) {
            return Optional.of(
                    getKeyManagerFactory(
                            config.get(HTTP_CLIENT_SSL_KEYSTORE_PATH).toString(),
                            config.get(HTTP_CLIENT_SSL_KEYSTORE_PASSWORD).toString().toCharArray(),
                            config.get(HTTP_CLIENT_SSL_KEYSTORE_TYPE).toString(),
                            config.get(HTTP_CLIENT_SSL_KEYSTORE_ALGORITHM).toString()));
        }
        return Optional.empty();
    }


     default KeyManagerFactory getKeyManagerFactory(String keyStorePath,
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

    static TrustManagerFactory getTrustManagerFactory(String trustStorePath,
                                                      char[] password,
                                                      @Nullable String keystoreType,
                                                      @Nullable String algorithm) {
        TrustManagerFactory trustManagerFactory;
        KeyStore trustStore;
        try {
            String finalAlgorithm = Optional.ofNullable(algorithm).orElse(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory = TrustManagerFactory.getInstance(finalAlgorithm);
            String finalKeystoreType = Optional.ofNullable(keystoreType).orElse(KeyStore.getDefaultType());
            trustStore = KeyStore.getInstance(finalKeystoreType);
        } catch (NoSuchAlgorithmException | KeyStoreException e) {
            throw new HttpException(e);
        }

        Path path = Path.of(trustStorePath);
        File file = path.toFile();
        try (InputStream inputStream = new FileInputStream(file)) {
            trustStore.load(inputStream, password);
            trustManagerFactory.init(trustStore);
        } catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException e) {
            throw new HttpException(e);
        }
        return trustManagerFactory;
    }

    static TrustManagerFactory getTrustManagerFactory(Map<String,Object> config){
        if(config.containsKey(HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST)&& Boolean.parseBoolean(config.get(HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST).toString())){
            LOGGER.warn("/!\\ activating 'always trust any certificate' feature : remote SSL certificates will always be granted. Use this feature at your own risk ! ");
            return new AlwaysTrustManagerFactory();
        }else {
            String trustStorePath = (String) config.get(HTTP_CLIENT_SSL_TRUSTSTORE_PATH);
            Preconditions.checkNotNull(trustStorePath, CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PATH + IS_NOT_SET);

            String truststorePassword = (String) config.get(HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD);
            Preconditions.checkNotNull(truststorePassword, CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD + IS_NOT_SET);

            String trustStoreType = (String) config.get(HTTP_CLIENT_SSL_TRUSTSTORE_TYPE);
            Preconditions.checkNotNull(trustStoreType, CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_TYPE + IS_NOT_SET);

            String truststoreAlgorithm = (String) config.get(HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM);
            Preconditions.checkNotNull(truststoreAlgorithm, HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM + IS_NOT_SET);

            return getTrustManagerFactory(
                    trustStorePath,
                    truststorePassword.toCharArray(),
                    trustStoreType,
                    truststoreAlgorithm);
        }
    }


    static Optional<TrustManagerFactory> buildTrustManagerFactory(Map<String, Object> config) {
        if ((config.containsKey(HTTP_CLIENT_SSL_TRUSTSTORE_PATH)
                && config.containsKey(HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD))||config.containsKey(HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST)) {

            Optional<TrustManagerFactory> trustManagerFactoryOption = Optional.ofNullable(
                    getTrustManagerFactory(config));
            if (trustManagerFactoryOption.isPresent()) {
                TrustManagerFactory myTrustManagerFactory = trustManagerFactoryOption.get();
                LOGGER.info("using trustManagerFactory class : {}",myTrustManagerFactory.getClass().getName());
                return Optional.of(myTrustManagerFactory);
            }
        }
        return Optional.empty();
    }
    static SSLSocketFactory getSSLSocketFactory(@Nullable KeyManagerFactory keyManagerFactory,
                                                       @Nullable TrustManagerFactory trustManagerFactory,
                                                       @Nullable String protocol,
                                                       SecureRandom random){
        try {
            SSLContext sslContext = SSLContext.getInstance(Optional.ofNullable(protocol).orElse(DEFAULT_SSL_PROTOCOL));
            sslContext.init(keyManagerFactory!=null?keyManagerFactory.getKeyManagers():null,trustManagerFactory!=null?trustManagerFactory.getTrustManagers():null, random);
            return sslContext.getSocketFactory();
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new HttpException(e);
        }
    }



}
