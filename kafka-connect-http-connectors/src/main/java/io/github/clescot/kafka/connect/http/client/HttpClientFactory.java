package io.github.clescot.kafka.connect.http.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.MapUtils;
import io.github.clescot.kafka.connect.http.client.proxy.ProxySelectorFactory;
import io.github.clescot.kafka.connect.http.client.ssl.AlwaysTrustManagerFactory;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import jakarta.annotation.Nullable;
import org.jetbrains.annotations.NotNull;
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
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;

import static io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition.*;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.CONFIG_DEFAULT_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX;
import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.SUCCESS_RESPONSE_CODE_REGEX;

/**
 * Factory to build a HttpClient.
 *
 * @param <C> client type, which is a subclass of HttpClient
 * @param <R> native HttpRequest
 * @param <S> native HttpResponse
 */
public interface HttpClientFactory<C extends HttpClient<R, S>, R, S> {
    String SHA_1_PRNG = "SHA1PRNG";
    Logger LOGGER = LoggerFactory.getLogger(HttpClientFactory.class);
    String DEFAULT_SSL_PROTOCOL = "SSL";
    String IS_NOT_SET = " is not set";
    Pattern defaultSuccessPattern = Pattern.compile(CONFIG_DEFAULT_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX);
    String CONFIGURATION_ID = "configuration.id";
    C build(Map<String, String> config,
            ExecutorService executorService,
            Random random,
            Proxy proxy,
            ProxySelector proxySelector,
            CompositeMeterRegistry meterRegistry);

    default C buildHttpClient(Map<String, String> config,
                              ExecutorService executorService,
                              CompositeMeterRegistry meterRegistry,
                              Random random) {
        Preconditions.checkNotNull(random, "Random must not be null.");
        Preconditions.checkNotNull(meterRegistry, "MeterRegistry must not be null.");
        //get proxy
        Proxy proxy = null;
        if (config.containsKey(PROXY_HTTP_CLIENT_HOSTNAME)) {
            String proxyHostName = config.get(PROXY_HTTP_CLIENT_HOSTNAME);
            int proxyPort = Integer.parseInt(config.get(PROXY_HTTP_CLIENT_PORT));
            SocketAddress socketAddress = new InetSocketAddress(proxyHostName, proxyPort);
            String proxyTypeLabel = Optional.ofNullable(config.get(PROXY_HTTP_CLIENT_TYPE)).orElse("HTTP");
            Proxy.Type proxyType = Proxy.Type.valueOf(proxyTypeLabel);
            proxy = new Proxy(proxyType, socketAddress);
        }

        ProxySelectorFactory proxySelectorFactory = new ProxySelectorFactory();
        ProxySelector proxySelector = null;
        if (config.get(PROXY_SELECTOR_HTTP_CLIENT_0_HOSTNAME) != null) {
            proxySelector = proxySelectorFactory.build(config, random);
        }


        C httpClient = build(config, executorService, random, proxy, proxySelector, meterRegistry);

        //enrich exchange
        //success response code regex
        Pattern successResponseCodeRegex;
        if (config.containsKey(SUCCESS_RESPONSE_CODE_REGEX)) {
            successResponseCodeRegex = Pattern.compile(config.get(SUCCESS_RESPONSE_CODE_REGEX));
        } else {
            successResponseCodeRegex = defaultSuccessPattern;
        }
        httpClient.setAddSuccessStatusToHttpExchangeFunction(successResponseCodeRegex);


        return httpClient;
    }


    default Optional<KeyManagerFactory> getKeyManagerFactory(Map<String, String> config) {
        if (config.containsKey(HTTP_CLIENT_SSL_KEYSTORE_PATH)
                && config.containsKey(HTTP_CLIENT_SSL_KEYSTORE_PASSWORD)) {
            return Optional.of(
                    getKeyManagerFactory(
                            config.get(HTTP_CLIENT_SSL_KEYSTORE_PATH),
                            config.get(HTTP_CLIENT_SSL_KEYSTORE_PASSWORD).toCharArray(),
                            config.get(HTTP_CLIENT_SSL_KEYSTORE_TYPE),
                            config.get(HTTP_CLIENT_SSL_KEYSTORE_ALGORITHM)));
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
            throw new IllegalArgumentException(e);
        }

        Path path = Path.of(keyStorePath);
        File file = path.toFile();
        try (InputStream inputStream = new FileInputStream(file)) {
            keyStore.load(inputStream, password);
            keyManagerFactory.init(keyStore, password);
        } catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException |
                 UnrecoverableKeyException e) {
            throw new IllegalStateException(e);
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
            throw new IllegalArgumentException(e);
        }

        Path path = Path.of(trustStorePath);
        File file = path.toFile();
        try (InputStream inputStream = new FileInputStream(file)) {
            trustStore.load(inputStream, password);
            trustManagerFactory.init(trustStore);
        } catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException e) {
            throw new IllegalStateException(e);
        }
        return trustManagerFactory;
    }

    static TrustManagerFactory getTrustManagerFactory(Map<String, String> config) {
        if (config.containsKey(HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST) && Boolean.parseBoolean(config.get(HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST))) {
            LOGGER.warn("/!\\ activating 'always trust any certificate' feature : remote SSL certificates will always be granted. Use this feature at your own risk ! ");
            return new AlwaysTrustManagerFactory();
        } else {
            String trustStorePath = config.get(HTTP_CLIENT_SSL_TRUSTSTORE_PATH);
            Preconditions.checkNotNull(trustStorePath, CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PATH + IS_NOT_SET);

            String truststorePassword = config.get(HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD);
            Preconditions.checkNotNull(truststorePassword, CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD + IS_NOT_SET);

            String trustStoreType = config.get(HTTP_CLIENT_SSL_TRUSTSTORE_TYPE);
            Preconditions.checkNotNull(trustStoreType, CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_TYPE + IS_NOT_SET);

            String truststoreAlgorithm = config.get(HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM);
            Preconditions.checkNotNull(truststoreAlgorithm, HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM + IS_NOT_SET);

            return getTrustManagerFactory(
                    trustStorePath,
                    truststorePassword.toCharArray(),
                    trustStoreType,
                    truststoreAlgorithm);
        }
    }


    static Optional<TrustManagerFactory> buildTrustManagerFactory(Map<String, String> config) {
        if ((config.containsKey(HTTP_CLIENT_SSL_TRUSTSTORE_PATH)
                && config.containsKey(HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD)) || config.containsKey(HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST)) {

            Optional<TrustManagerFactory> trustManagerFactoryOption = Optional.ofNullable(
                    getTrustManagerFactory(config));
            if (trustManagerFactoryOption.isPresent()) {
                TrustManagerFactory myTrustManagerFactory = trustManagerFactoryOption.get();
                LOGGER.info("using trustManagerFactory class : {}", myTrustManagerFactory.getClass().getName());
                return Optional.of(myTrustManagerFactory);
            }
        }
        return Optional.empty();
    }

    static SSLSocketFactory getSSLSocketFactory(@Nullable KeyManagerFactory keyManagerFactory,
                                                @Nullable TrustManagerFactory trustManagerFactory,
                                                @Nullable String protocol,
                                                SecureRandom random) {
        try {
            SSLContext sslContext = SSLContext.getInstance(Optional.ofNullable(protocol).orElse(DEFAULT_SSL_PROTOCOL));
            sslContext.init(keyManagerFactory != null ? keyManagerFactory.getKeyManagers() : null, trustManagerFactory != null ? trustManagerFactory.getTrustManagers() : null, random);
            return sslContext.getSocketFactory();
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new IllegalArgumentException(e);
        }
    }



    public static  <C extends HttpClient<R, S>, R, S>Map<String,C> buildConfigurations(
            HttpClientFactory<C, R, S> httpClientFactory,
            ExecutorService executorService,
            List<String> configIdList,
            Map<String, String> originals, CompositeMeterRegistry meterRegistry) {
        Map<String,C> httpClientConfigurations = Maps.newHashMap();
        List<String> configurationIds = Lists.newArrayList();
        Optional<List<String>> ids = Optional.ofNullable(configIdList);
        ids.ifPresent(configurationIds::addAll);
        for (String configId : configurationIds) {
            Map<String, String> config = Maps.newHashMap(MapUtils.getMapWithPrefix(originals, "config." + configId + "."));
            HashMap<String, String> settings = Maps.newHashMap(config);
            settings.put("configuration.id", configId);
            Random random = getRandom(settings);
            C httpClient = httpClientFactory.buildHttpClient(settings, executorService, meterRegistry, random);
            httpClientConfigurations.put(configId,httpClient);
        }
        return httpClientConfigurations;
    }

    @java.lang.SuppressWarnings({"java:S2119","java:S2245"})
    @NotNull
    static Random getRandom(Map<String, String> config) {
        Random random;

        try {
            if(config.containsKey(HTTP_CLIENT_SECURE_RANDOM_ACTIVATE)&&Boolean.parseBoolean(config.get(HTTP_CLIENT_SECURE_RANDOM_ACTIVATE))){
                String rngAlgorithm = SHA_1_PRNG;
                if (config.containsKey(HTTP_CLIENT_SECURE_RANDOM_PRNG_ALGORITHM)) {
                    rngAlgorithm = config.get(HTTP_CLIENT_SECURE_RANDOM_PRNG_ALGORITHM);
                }
                random = SecureRandom.getInstance(rngAlgorithm);
            }else {
                if(config.containsKey(HTTP_CLIENT_UNSECURE_RANDOM_SEED)){
                    long seed = Long.parseLong(config.get(HTTP_CLIENT_UNSECURE_RANDOM_SEED));
                    random = new Random(seed);
                }else {
                    random = new Random();
                }
            }
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(e);
        }
        return random;
    }


}
