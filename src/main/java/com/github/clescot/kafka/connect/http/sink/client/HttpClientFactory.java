package com.github.clescot.kafka.connect.http.sink.client;

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
import java.util.Map;
import java.util.Optional;

public interface HttpClientFactory {





    static TrustManagerFactory getTrustManagerFactory(String trustStorePath, char[] password, String keystoreType, String algorithm) {
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

    HttpClient build(Map<String, String> config);
}
