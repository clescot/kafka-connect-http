package io.github.clescot.kafka.connect.http.sink.client;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import dev.failsafe.RateLimiter;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.sink.client.ssl.AlwaysTrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

public interface HttpClient<Q, S> {
    boolean FAILURE = false;
    int SERVER_ERROR_STATUS_CODE = 500;
    String UTC_ZONE_ID = "UTC";
    boolean SUCCESS = true;
    int ONE_HTTP_REQUEST = 1;
    Logger LOGGER = LoggerFactory.getLogger(HttpClient.class);
    String IS_NOT_SET = " is not set";


    default HttpExchange buildHttpExchange(HttpRequest httpRequest,
                                           HttpResponse httpResponse,
                                           Stopwatch stopwatch,
                                           OffsetDateTime now,
                                           AtomicInteger attempts,
                                           boolean success) {
        Preconditions.checkNotNull(httpRequest, "'httpRequest' is null");
        return HttpExchange.Builder.anHttpExchange()
                //request
                .withHttpRequest(httpRequest)
                //response
                .withHttpResponse(httpResponse)
                //technical metadata
                //time elapsed during http call
                .withDuration(stopwatch.elapsed(TimeUnit.MILLISECONDS))
                //at which moment occurs the beginning of the http call
                .at(now)
                .withAttempts(attempts)
                .withSuccess(success)
                .build();
    }


    /**
     * convert an {@link HttpRequest} into a native (from the implementation) request.
     *
     * @param httpRequest http request to build.
     * @return native request.
     */

    Q buildRequest(HttpRequest httpRequest);

    default CompletableFuture<HttpExchange> call(HttpRequest httpRequest, AtomicInteger attempts) throws HttpException {

        Stopwatch stopwatch = Stopwatch.createStarted();
        CompletableFuture<S> response;
        LOGGER.info("httpRequest: {}", httpRequest);
        Q request = buildRequest(httpRequest);
        LOGGER.info("native request: {}", request);
        OffsetDateTime now = OffsetDateTime.now(ZoneId.of(UTC_ZONE_ID));
        response = call(request);
        Preconditions.checkNotNull(response, "response is null");
        LOGGER.info("native response: {}", response);
        return response.thenApply(this::buildResponse)
                .thenApply(myResponse -> {
                            stopwatch.stop();
                            LOGGER.info("httpResponse: {}", myResponse);
                            LOGGER.info("duration: {}ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
                            return buildHttpExchange(httpRequest, myResponse, stopwatch, now, attempts, myResponse.getStatusCode() < 400 ? SUCCESS : FAILURE);
                        }
                ).exceptionally((throwable-> {
                    HttpResponse httpResponse = new HttpResponse(400,throwable.getMessage());
                    return buildHttpExchange(httpRequest, httpResponse, stopwatch, now, attempts,FAILURE);
                }));

    }

    CompletableFuture<S> call(Q request);

    /**
     * convert a native response (from the implementation) to an {@link HttpResponse}.
     *
     * @param response native response
     * @return HttpResponse
     */

    HttpResponse buildResponse(S response);

    CompletableFuture<S> nativeCall(Q request);

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
        if(config.containsKey(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST)&& Boolean.TRUE.equals(config.get(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALWAYS_TRUST))){
            return new AlwaysTrustManagerFactory();
        }else {
            String trustStorePath = (String) config.get(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PATH);
            Preconditions.checkNotNull(trustStorePath, CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PATH + IS_NOT_SET);

            String truststorePassword = (String) config.get(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD);
            Preconditions.checkNotNull(truststorePassword, CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_PASSWORD + IS_NOT_SET);

            String trustStoreType = (String) config.get(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_TYPE);
            Preconditions.checkNotNull(trustStoreType, CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_TYPE + IS_NOT_SET);

            String truststoreAlgorithm = (String) config.get(CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM);
            Preconditions.checkNotNull(truststoreAlgorithm, CONFIG_HTTP_CLIENT_SSL_TRUSTSTORE_ALGORITHM + IS_NOT_SET);

            return getTrustManagerFactory(
                    trustStorePath,
                    truststorePassword.toCharArray(),
                    trustStoreType,
                    truststoreAlgorithm);
        }
    }


    void setRateLimiter(RateLimiter<HttpExchange> rateLimiter);

    Optional<RateLimiter<HttpExchange>> getRateLimiter();
}
