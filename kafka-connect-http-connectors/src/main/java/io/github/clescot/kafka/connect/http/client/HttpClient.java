package io.github.clescot.kafka.connect.http.client;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dev.failsafe.RateLimiter;
import io.github.clescot.kafka.connect.http.client.ssl.AlwaysTrustManagerFactory;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.core.HttpResponseBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.Nullable;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

/**
 * execute the HTTP call.
 * @param <Q> native HttpRequest
 * @param <S> native HttpResponse
 */
public interface HttpClient<Q, S> {
    boolean FAILURE = false;
    int SERVER_ERROR_STATUS_CODE = 500;
    String UTC_ZONE_ID = "UTC";
    boolean SUCCESS = true;
    int ONE_HTTP_REQUEST = 1;
    Logger LOGGER = LoggerFactory.getLogger(HttpClient.class);
    String IS_NOT_SET = " is not set";
    String THROWABLE_CLASS = "throwable.class";
    String THROWABLE_MESSAGE = "throwable.message";


    static HttpExchange buildHttpExchange(HttpRequest httpRequest,
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

        Stopwatch rateLimitedStopWatch = Stopwatch.createStarted();
        CompletableFuture<S> response;
        LOGGER.debug("httpRequest: {}", httpRequest);
        Q request = buildRequest(httpRequest);
        LOGGER.debug("native request: {}", request);
        OffsetDateTime now = OffsetDateTime.now(ZoneId.of(UTC_ZONE_ID));
        try {
            Optional<RateLimiter<HttpExchange>> limiter = getRateLimiter();
            if (limiter.isPresent()) {
                RateLimiter<HttpExchange> httpExchangeRateLimiter = limiter.get();
                if(RATE_LIMITER_REQUEST_LENGTH_PER_CALL.equals(getPermitsPerCall())){
                    httpExchangeRateLimiter.acquirePermits(Math.toIntExact(httpRequest.getLength()));
                }else{
                    httpExchangeRateLimiter.acquirePermits(HttpClient.ONE_HTTP_REQUEST);
                    LOGGER.trace("permits acquired request:'{}'", request);
                }
            }else{
                LOGGER.trace("no rate limiter is configured");
            }
            Stopwatch directStopWatch = Stopwatch.createStarted();
            response = nativeCall(request);

        Preconditions.checkNotNull(response, "response is null");
        HttpResponseBuilder httpResponseBuilder = new HttpResponseBuilder(getStatusMessageLimit(),getHeadersLimit(), getBodyLimit());
        return response.thenApply((res)->buildResponse(httpResponseBuilder,res))
                .thenApply(myResponse -> {
                            directStopWatch.stop();
                            rateLimitedStopWatch.stop();
                            if(LOGGER.isTraceEnabled()) {
                                LOGGER.trace("httpResponse: {}", myResponse);
                            }
                    Integer responseStatusCode = myResponse.getStatusCode();
                    String responseStatusMessage = myResponse.getStatusMessage();
                    long directElaspedTime = directStopWatch.elapsed(TimeUnit.MILLISECONDS);
                    //elapsed time contains rate limiting waiting time + + local code execution time + network time + remote server-side execution time
                    long overallElapsedTime = rateLimitedStopWatch.elapsed(TimeUnit.MILLISECONDS);
                    long waitingTime = overallElapsedTime - directElaspedTime;
                    LOGGER.info("[{}] {} {} : {} '{}' (direct : '{}' ms, waiting time :'{}'ms overall : '{}' ms)",Thread.currentThread().getId(),httpRequest.getMethod(),httpRequest.getUrl(),responseStatusCode,responseStatusMessage, directElaspedTime,waitingTime,overallElapsedTime);
                    return buildHttpExchange(httpRequest, myResponse, directStopWatch, now, attempts, responseStatusCode < 400 ? SUCCESS : FAILURE);
                        }
                ).exceptionally((throwable-> {
                    HttpResponse httpResponse = new HttpResponse(400,throwable.getMessage());
                    Map<String, List<String>> responseHeaders = Maps.newHashMap();
                    responseHeaders.put(THROWABLE_CLASS, Lists.newArrayList(throwable.getCause().getClass().getName()));
                    responseHeaders.put(THROWABLE_MESSAGE, Lists.newArrayList(throwable.getCause().getMessage()));
                    httpResponse.setHeaders(responseHeaders);
                    LOGGER.error(throwable.toString());
                    return buildHttpExchange(httpRequest, httpResponse, rateLimitedStopWatch, now, attempts,FAILURE);
                }));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HttpException(e);
        }
    }



    /**
     * convert a native response (from the implementation) to an {@link HttpResponse}.
     *
     * @param response native response
     * @return HttpResponse
     */

    HttpResponse buildResponse(HttpResponseBuilder httpResponseBuilder,S response);

    /**
     * raw native HttpRequest call.
     * @param request native HttpRequest
     * @return CompletableFuture of a native HttpResponse.
     */
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

    Integer getStatusMessageLimit();

    void setStatusMessageLimit(Integer statusMessageLimit);

    Integer getHeadersLimit();

    void setHeadersLimit(Integer headersLimit);

    Integer getBodyLimit();

    String getPermitsPerCall();

    void setBodyLimit(Integer bodyLimit);

    void setRateLimiter(RateLimiter<HttpExchange> rateLimiter);

    Optional<RateLimiter<HttpExchange>> getRateLimiter();

    TrustManagerFactory getTrustManagerFactory();

    String getEngineId();
}
