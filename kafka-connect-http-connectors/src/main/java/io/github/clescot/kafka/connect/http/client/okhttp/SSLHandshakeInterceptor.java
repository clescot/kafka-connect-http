package io.github.clescot.kafka.connect.http.client.okhttp;

import okhttp3.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.Principal;
import java.security.cert.Certificate;
import java.util.List;

public class SSLHandshakeInterceptor implements Interceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SSLHandshakeInterceptor.class);

    @NotNull
    @Override
    public Response intercept(@NotNull Chain chain) throws IOException {
        Request request = chain.request();
        Handshake handshake = chain.connection().handshake();
        if(handshake!=null) {
            //cipher
            CipherSuite cipherSuite = handshake.cipherSuite();
            if (cipherSuite != null) {
                String cipherSuiteJavaName = cipherSuite.javaName();
                LOGGER.debug("cipherSuiteJavaName:{}", cipherSuiteJavaName);
            }
            //local
            Principal localPrincipal = handshake.localPrincipal();
            if (localPrincipal != null) {
                String localPrincipalName = localPrincipal.getName();
                LOGGER.debug("local principal:{}", localPrincipalName);
            }
            List<Certificate> localCertificates = handshake.localCertificates();
            for (Certificate localCertificate : localCertificates) {
                LOGGER.debug("local certificate:{}", localCertificate);
            }
            //remote
            Principal peerPrincipal = handshake.peerPrincipal();
            if (peerPrincipal != null) {
                String peerPrincipalName = peerPrincipal.getName();
                LOGGER.debug("PEER principal:{}", peerPrincipalName);
            }
            List<Certificate> peerCertificates = handshake.peerCertificates();
            for (Certificate peerCertificate : peerCertificates) {
                LOGGER.debug("peer certificate:{}", peerCertificate);
            }
        }

        Response response = chain.proceed(request);
        return response;
    }
}
