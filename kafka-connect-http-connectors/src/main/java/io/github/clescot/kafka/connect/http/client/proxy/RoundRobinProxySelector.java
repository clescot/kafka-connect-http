package io.github.clescot.kafka.connect.http.client.proxy;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.util.Iterator;
import java.util.List;

public class RoundRobinProxySelector extends ProxySelector {

    private final Iterator<Proxy> proxies;

    public RoundRobinProxySelector(List<Proxy> proxies) {
        Preconditions.checkNotNull(proxies,"proxies list must not be null");
        Preconditions.checkArgument(!proxies.isEmpty(),"proxies list must not be empty");
        this.proxies = Iterables.cycle(proxies).iterator();
    }

    @Override
    public List<Proxy> select(URI uri) {
        return List.of(proxies.next());
    }

    @Override
    public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {

    }
}
