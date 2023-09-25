package io.github.clescot.kafka.connect.http.client.proxy;


import com.google.common.base.Preconditions;

import java.io.IOException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Random;

public class RandomProxySelector extends ProxySelector {

    private final List<Proxy> proxies;
    private final Random random;

    public RandomProxySelector(List<Proxy> proxies, Random random) {
        Preconditions.checkNotNull(proxies,"proxies list must not be null");
        Preconditions.checkArgument(!proxies.isEmpty(),"proxies list must not be empty");
        Preconditions.checkNotNull(random,"random must not be null");
        this.proxies = proxies;
        this.random = random;
    }

    @Override
    public List<Proxy> select(URI uri) {
        return List.of(proxies.get(random.nextInt(proxies.size())));
    }

    @Override
    public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {

    }
}
