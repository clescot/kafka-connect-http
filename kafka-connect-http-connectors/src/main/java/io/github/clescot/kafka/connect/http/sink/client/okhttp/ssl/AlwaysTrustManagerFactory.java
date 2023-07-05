package io.github.clescot.kafka.connect.http.sink.client.okhttp.ssl;

import javax.net.ssl.TrustManagerFactory;

public class AlwaysTrustManagerFactory extends TrustManagerFactory {
    public AlwaysTrustManagerFactory() {
        super(new AlwaysTrustManagerFactorySpi(), null, null);
    }
}
