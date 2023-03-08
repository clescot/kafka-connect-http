package io.github.clescot.kafka.connect.http.sink.client;

import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public interface HttpClient<Req, Res> {
    boolean FAILURE = false;
    int SERVER_ERROR_STATUS_CODE = 500;
    String BLANK_RESPONSE_CONTENT = "";
    String UTC_ZONE_ID = "UTC";
    boolean SUCCESS = true;
    int ONE_HTTP_REQUEST = 1;
    Logger LOGGER = LoggerFactory.getLogger(HttpClient.class);



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
     * @param httpRequest
     * @return native request.
     */

    Req buildRequest(HttpRequest httpRequest);

    default HttpExchange call(HttpRequest httpRequest, AtomicInteger attempts) throws HttpException {

        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            LOGGER.info("httpRequest: {}", httpRequest);
            Req request = buildRequest(httpRequest);
            LOGGER.info("native request: {}", request);
            OffsetDateTime now = OffsetDateTime.now(ZoneId.of(UTC_ZONE_ID));
            Res response = nativeCall(request);
            LOGGER.info("native response: {}", response);
            stopwatch.stop();
            HttpResponse httpResponse = buildResponse(response);
            LOGGER.info("httpResponse: {}", response);
            LOGGER.info("duration: {}", stopwatch);
            return buildHttpExchange(httpRequest, httpResponse, stopwatch, now, attempts,httpResponse.getStatusCode()<400?SUCCESS:FAILURE);
        } catch (HttpException e) {
            LOGGER.error("Failed to call web service {} ", e.getMessage());
            throw new HttpException(e.getMessage());
        } finally {
            if (stopwatch.isRunning()) {
                stopwatch.stop();
            }
        }
    }

    /**
     * convert a native response (from the implementation) to an {@link HttpResponse}.
     * @param response native response
     * @return HttpResponse
     */

    HttpResponse buildResponse(Res response);
    Res nativeCall(Req request);



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
            throw new RuntimeException(e);
        }

        Path path = Path.of(trustStorePath);
        File file = path.toFile();
        try (InputStream inputStream = new FileInputStream(file)) {
            trustStore.load(inputStream, password);
            trustManagerFactory.init(trustStore);
        } catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException e) {
            throw new RuntimeException(e);
        }
        return trustManagerFactory;
    }
}
