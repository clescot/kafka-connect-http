package io.github.clescot.kafka.connect.http.sink.client;

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
import java.util.Map;
import java.util.Optional;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;

public abstract class AbstractHttpClient<Req, Res> implements HttpClient<Req, Res> {

    private static final String DEFAULT_SSL_PROTOCOL = "SSL";
    protected Map<String, String> config;

    public AbstractHttpClient(Map<String, String> config) {
        this.config = config;
    }

    protected Optional<TrustManagerFactory> getTrustManagerFactory() {
        if (config.containsKey(HTTPCLIENT_SSL_TRUSTSTORE_PATH) && config.containsKey(HTTPCLIENT_SSL_TRUSTSTORE_PASSWORD)) {

            Optional<TrustManagerFactory> trustManagerFactory = Optional.ofNullable(
                    HttpClient.getTrustManagerFactory(
                            config.get(HTTPCLIENT_SSL_TRUSTSTORE_PATH),
                            config.get(HTTPCLIENT_SSL_TRUSTSTORE_PASSWORD).toCharArray(),
                            config.get(HTTPCLIENT_SSL_TRUSTSTORE_TYPE),
                            config.get(HTTPCLIENT_SSL_TRUSTSTORE_ALGORITHM)));
            if (trustManagerFactory.isPresent()) {
                return Optional.of(trustManagerFactory.get());
            }
        }
        return Optional.empty();
    }

    protected Optional<KeyManagerFactory> getKeyManagerFactory() {
        if (config.containsKey(HTTPCLIENT_SSL_KEYSTORE_PATH) && config.containsKey(HTTPCLIENT_SSL_KEYSTORE_PASSWORD)) {

            Optional<KeyManagerFactory> keyManagerFactory = Optional.ofNullable(
                    getKeyManagerFactory(
                            config.get(HTTPCLIENT_SSL_KEYSTORE_PATH),
                            config.get(HTTPCLIENT_SSL_KEYSTORE_PASSWORD).toCharArray(),
                            config.get(HTTPCLIENT_SSL_KEYSTORE_TYPE),
                            config.get(HTTPCLIENT_SSL_KEYSTORE_ALGORITHM)));
            if (keyManagerFactory.isPresent()) {
                return Optional.of(keyManagerFactory.get());
            }
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
            throw new RuntimeException(e);
        }

        Path path = Path.of(keyStorePath);
        File file = path.toFile();
        try (InputStream inputStream = new FileInputStream(file)) {
            keyStore.load(inputStream, password);
            keyManagerFactory.init(keyStore,password);
        } catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException |
                 UnrecoverableKeyException e) {
            throw new RuntimeException(e);
        }
        return keyManagerFactory;
    }


    protected static SSLSocketFactory getSSLSocketFactory(@Nullable KeyManagerFactory keyManagerFactory,
                                                          @Nullable TrustManagerFactory trustManagerFactory,
                                                @Nullable String protocol){
        try {
            SSLContext sslContext = SSLContext.getInstance(Optional.ofNullable(protocol).orElse(DEFAULT_SSL_PROTOCOL));
            SecureRandom random = new SecureRandom();
            sslContext.init(keyManagerFactory!=null?keyManagerFactory.getKeyManagers():null,trustManagerFactory!=null?trustManagerFactory.getTrustManagers():null, random);
            return sslContext.getSocketFactory();
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new RuntimeException(e);
        }
    }

}
