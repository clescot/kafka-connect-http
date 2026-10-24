package io.github.clescot.client.http.ssl;

import javax.net.ssl.TrustManagerFactory;

public class AlwaysTrustManagerFactory extends TrustManagerFactory {
    public AlwaysTrustManagerFactory() {
        super(new AlwaysTrustManagerFactorySpi(), null, null);
    }
}
