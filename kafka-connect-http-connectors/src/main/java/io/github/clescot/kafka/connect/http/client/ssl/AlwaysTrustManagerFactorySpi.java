package io.github.clescot.kafka.connect.http.client.ssl;

import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactorySpi;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
import java.security.KeyStoreException;

public class AlwaysTrustManagerFactorySpi extends TrustManagerFactorySpi {
    @Override
    protected void engineInit(KeyStore ks) throws KeyStoreException {
        //no init is needed as we only build AlwaysTrustManager instance
    }

    @Override
    protected void engineInit(ManagerFactoryParameters spec) throws InvalidAlgorithmParameterException {
        //no init is needed as we only build AlwaysTrustManager instance
    }

    @Override
    protected TrustManager[] engineGetTrustManagers() {
        return new TrustManager[]{new AlwaysTrustManager()};
    }
}
