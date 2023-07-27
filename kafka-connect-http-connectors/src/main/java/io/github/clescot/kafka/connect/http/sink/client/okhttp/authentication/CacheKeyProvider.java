package io.github.clescot.kafka.connect.http.sink.client.okhttp.authentication;

public interface CacheKeyProvider<T> {

    boolean applyToProxy();
    /**
     * Provides the caching key for the given request. Can be used to share passwords accross multiple subdomains.
     *
     * @param request the http request.
     * @return the cache key.
     */
    String getCachingKey(T request);
}

