package io.github.clescot.kafka.connect.http.client.okhttp.interceptor;

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
        Connection connection = chain.connection();
        if (connection != null) {
            Handshake handshake = connection.handshake();
            if (handshake != null) {
                //cipher
                CipherSuite cipherSuite = handshake.cipherSuite();
                if (cipherSuite != null) {
                    String cipherSuiteJavaName = cipherSuite.javaName();
                    LOGGER.debug("cipherSuiteJavaName:{}", cipherSuiteJavaName);
                }
                displayLocalInformations(handshake);
                displayRemoteInformations(handshake);
            }
        }

        return chain.proceed(request);
    }

    private void displayRemoteInformations(Handshake handshake) {
        //remote
        Principal peerPrincipal = handshake.peerPrincipal();
        if (peerPrincipal != null) {
            String peerPrincipalName = peerPrincipal.getName();
            LOGGER.debug("peer principal: {}", peerPrincipalName);
        }
        List<Certificate> peerCertificates = handshake.peerCertificates();
        LOGGER.debug("peer certificates size:{}", peerCertificates.size());
        for (Certificate peerCertificate : peerCertificates) {
            LOGGER.debug("peer certificate: {}", peerCertificate);
        }
    }

    private void displayLocalInformations(Handshake handshake) {
        //local
        Principal localPrincipal = handshake.localPrincipal();
        if (localPrincipal != null) {
            String localPrincipalName = localPrincipal.getName();
            LOGGER.debug("local principal: {}", localPrincipalName);
        }
        List<Certificate> localCertificates = handshake.localCertificates();
        LOGGER.debug("local certificates size: {}", localCertificates.size());
        for (Certificate localCertificate : localCertificates) {
            LOGGER.debug("local certificate: {}", localCertificate);
        }
    }
}
