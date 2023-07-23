package io.github.clescot.kafka.connect.http.sink.client.proxy;

import java.io.IOException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;

public class WeightedRoundRobinProxySelector extends ProxySelector {

    private final NavigableMap<Integer, Proxy> proxies;
    private final Random random;
    private final int totalWeight;

    public WeightedRoundRobinProxySelector(NavigableMap<Integer,Proxy> proxies, Random random) {
        this.proxies = proxies;
        this.random = random;
        this.totalWeight = proxies.keySet().stream().reduce(0, Integer::sum);
    }

    @Override
    public List<Proxy> select(URI uri) {
        return List.of(proxies.ceilingEntry(random.nextInt(totalWeight)).getValue());
    }

    @Override
    public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {

    }
}
