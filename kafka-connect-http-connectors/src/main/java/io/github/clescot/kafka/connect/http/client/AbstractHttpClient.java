package io.github.clescot.kafka.connect.http.client;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import dev.failsafe.RateLimiter;
import dev.failsafe.RateLimiterConfig;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.github.clescot.kafka.connect.http.client.Configuration.CONFIGURATION_ID;
import static io.github.clescot.kafka.connect.http.client.Configuration.STATIC_SCOPE;
import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

public abstract class AbstractHttpClient<Req, Res> implements HttpClient<Req, Res> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractHttpClient.class);
    private static final String DEFAULT_SSL_PROTOCOL = "SSL";
    protected Map<String, Object> config;
    private Optional<RateLimiter<HttpExchange>> rateLimiter = Optional.empty();
    protected TrustManagerFactory trustManagerFactory;
    protected  String configurationId;

    //rate limiter
    private final static Map<String, RateLimiter<HttpExchange>> sharedRateLimiters = Maps.newHashMap();

    protected AbstractHttpClient(Map<String, Object> config) {
        this.config = config;
        configurationId = (String) config.get(CONFIGURATION_ID);
        Preconditions.checkNotNull(configurationId,"configuration must have an id");
        setRateLimiter(buildRateLimiter(config));
    }

    protected Optional<TrustManagerFactory> buildTrustManagerFactory() {
        if ((config.containsKey(HTTP_CLIENT_SSL_TRUSTSTORE_PATH)
                && config.containsKey(HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD))||config.containsKey(HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST)) {

            Optional<TrustManagerFactory> trustManagerFactoryOption = Optional.ofNullable(
                    HttpClient.getTrustManagerFactory(config));
            if (trustManagerFactoryOption.isPresent()) {
                TrustManagerFactory myTrustManagerFactory = trustManagerFactoryOption.get();
                LOGGER.info("using trustManagerFactory class : {}",myTrustManagerFactory.getClass().getName());
                return Optional.of(myTrustManagerFactory);
            }
        }
        return Optional.empty();
    }

    protected Optional<KeyManagerFactory> getKeyManagerFactory() {
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

    @Override
    public CompletableFuture<Res> call(Req request){
        try {
            Optional<RateLimiter<HttpExchange>> limiter = getRateLimiter();
            if (limiter.isPresent()) {
                limiter.get().acquirePermits(HttpClient.ONE_HTTP_REQUEST);
                LOGGER.trace("permits acquired request:'{}'", request);
            }else{
                LOGGER.trace("no rate limiter is configured");
            }
            return nativeCall(request);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HttpException(e);
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


    @Override
    public TrustManagerFactory getTrustManagerFactory() {
        return trustManagerFactory;
    }

    public abstract Object getInternalClient();

    @Override
    public String toString() {
        return this.getClass().getSimpleName()+"{" +
                "rateLimiter=" + rateLimiterToString() +
                ", trustManagerFactory=" + trustManagerFactory +
                '}';
    }

    private String rateLimiterToString(){
        StringBuilder result = new StringBuilder("{");

        String rateLimiterMaxExecutions = (String) config.get(RATE_LIMITER_MAX_EXECUTIONS);
        if(rateLimiterMaxExecutions!=null){
            result.append("rateLimiterMaxExecutions:'").append(rateLimiterMaxExecutions).append("'");
        }
        String rateLimiterPeriodInMs = (String) config.get(RATE_LIMITER_PERIOD_IN_MS);
        if(rateLimiterPeriodInMs!=null){
            result.append(",rateLimiterPeriodInMs:'").append(rateLimiterPeriodInMs).append("'");
        }
        String rateLimiterScope = (String) config.get(RATE_LIMITER_SCOPE);
        if(rateLimiterScope!=null){
            result.append(",rateLimiterScope:'").append(rateLimiterScope).append("'");
        }
        result.append("}");
        return result.toString();
    }

    private RateLimiter<HttpExchange> buildRateLimiter(Map<String, Object> configMap) {
        String configurationId = (String) configMap.get(CONFIGURATION_ID);
        RateLimiter<HttpExchange> rateLimiter = null;
        if (configMap.containsKey(RATE_LIMITER_MAX_EXECUTIONS)) {
            long maxExecutions = Long.parseLong((String) configMap.get(RATE_LIMITER_MAX_EXECUTIONS));
            LOGGER.trace("configuration '{}' : maxExecutions :{}",configurationId,maxExecutions);
            long periodInMs = Long.parseLong(Optional.ofNullable((String) configMap.get(RATE_LIMITER_PERIOD_IN_MS)).orElse(1000 + ""));
            LOGGER.trace("configuration '{}' : periodInMs :{}",configurationId,periodInMs);
            if (configMap.containsKey(RATE_LIMITER_SCOPE) && STATIC_SCOPE.equalsIgnoreCase((String) configMap.get(RATE_LIMITER_SCOPE))) {
                LOGGER.trace("configuration '{}' : rateLimiter scope is 'static'",configurationId);
                Optional<RateLimiter<HttpExchange>> sharedRateLimiter = Optional.ofNullable(sharedRateLimiters.get(configurationId));
                if (sharedRateLimiter.isPresent()) {
                    rateLimiter = sharedRateLimiter.get();
                    LOGGER.trace("configuration '{}' : rate limiter is already shared",configurationId);
                } else {
                    rateLimiter = RateLimiter.<HttpExchange>smoothBuilder(maxExecutions, Duration.of(periodInMs, ChronoUnit.MILLIS)).build();
                    registerRateLimiter(configurationId, rateLimiter);
                }
            } else {
                rateLimiter = RateLimiter.<HttpExchange>smoothBuilder(maxExecutions, Duration.of(periodInMs, ChronoUnit.MILLIS)).build();
            }
            RateLimiterConfig<HttpExchange> config = rateLimiter.getConfig();
            LOGGER.trace("configuration '{}' rate limiter configured : 'maxRate:{},maxPermits{},period:{},maxWaitTime:{}'",configurationId,config.getMaxRate(),config.getMaxPermits(),config.getPeriod(),config.getMaxWaitTime());
        }else{
            if(LOGGER.isTraceEnabled()) {
                LOGGER.trace("configuration '{}' : rate limiter is not configured", configurationId);
                LOGGER.trace(Joiner.on(",\n").withKeyValueSeparator("=").join(configMap.entrySet()));
            }
        }
        return rateLimiter;
    }


    public static void registerRateLimiter(String configurationId, RateLimiter<HttpExchange> rateLimiter) {
        Preconditions.checkNotNull(configurationId, "we cannot register a rateLimiter for a 'null' configurationId");
        Preconditions.checkNotNull(rateLimiter, "we cannot register a 'null' rate limiter for the configurationId " + configurationId);
        LOGGER.info("registration of a shared rateLimiter for the configurationId '{}'", configurationId);
        sharedRateLimiters.put(configurationId, rateLimiter);
    }
}
