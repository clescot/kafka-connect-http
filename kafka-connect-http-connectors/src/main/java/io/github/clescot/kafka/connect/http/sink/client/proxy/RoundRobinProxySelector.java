package io.github.clescot.kafka.connect.http.sink.client.proxy;


import java.io.IOException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Random;

public class RoundRobinProxySelector extends ProxySelector {

    private final List<Proxy> proxies;
    private final Random random;

    public RoundRobinProxySelector(List<Proxy> proxies, Random random) {
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
