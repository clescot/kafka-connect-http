package io.github.clescot.kafka.connect.http.sink.client.okhttp.ssl;

import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactorySpi;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
import java.security.KeyStoreException;

public class AlwaysTrustManagerFactorySpi extends TrustManagerFactorySpi {
    @Override
    protected void engineInit(KeyStore ks) throws KeyStoreException {

    }

    @Override
    protected void engineInit(ManagerFactoryParameters spec) throws InvalidAlgorithmParameterException {

    }

    @Override
    protected TrustManager[] engineGetTrustManagers() {
        return new TrustManager[]{new AlwaysTrustManager()};
    }
}
