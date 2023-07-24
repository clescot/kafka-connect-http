package io.github.clescot.kafka.connect.http.sink.client.okhttp.authentication;

import java.net.Proxy;

/**
 * The default version of the cache key provider, which simply calls the java.net.Proxy.toString() method to generate key.
 */
public final class DefaultProxyCacheKeyProvider implements CacheKeyProvider<Proxy> {
    @Override
    public boolean applyToProxy() {
        return true;
    }

    /**
     *
     * @param proxy used to calculate the key.
     * @return the cache key.
     */
    @Override
    public String getCachingKey(Proxy proxy) {
        return proxy!=null?proxy.toString():null;
    }
}
