package io.github.clescot.kafka.connect.http.client.ssl;

import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * Use of this class cause is security issue. Use it at your own risk !
 * It should be used only for tests.
 */
public class AlwaysTrustManager implements X509TrustManager {
    @SuppressWarnings("java:S4830")
    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        // method is empty to accept all client certificates
    }

    @SuppressWarnings("java:S4830")
    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            // method is empty to accept all server certificates
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return new java.security.cert.X509Certificate[]{};
    }
}
