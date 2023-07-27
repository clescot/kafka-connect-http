package io.github.clescot.kafka.connect.http.sink.client.proxy;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.util.List;

public class HostHashProxySelector extends ProxySelector {

    private final List<Proxy> proxies;

    public HostHashProxySelector(List<Proxy> proxies) {
        Preconditions.checkNotNull(proxies,"proxies list must not be null");
        Preconditions.checkArgument(!proxies.isEmpty(),"proxies list must not be empty");
        this.proxies = proxies;
    }

    @Override
    public List<Proxy> select(URI uri) {
        return List.of(proxies.get(uri.getHost().hashCode()%(proxies.size())));

    }

    @Override
    public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {

    }
}
