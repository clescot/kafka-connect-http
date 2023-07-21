package io.github.clescot.kafka.connect.http.sink.client.okhttp;

import java.net.Proxy;

/**
 * The default version of the cache key provider, which simply takes the request URL / port for
 */
public final class DefaultProxyCacheKeyProvider implements CacheKeyProvider<Proxy> {
    @Override
    public boolean applyToProxy() {
        return true;
    }

    /**
     *
     * @param proxy
     * @return the cache key.
     */
    @Override
    public String getCachingKey(Proxy proxy) {
        return proxy!=null?proxy.toString():null;
    }
}
