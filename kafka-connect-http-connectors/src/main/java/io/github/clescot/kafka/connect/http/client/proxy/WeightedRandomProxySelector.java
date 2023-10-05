package io.github.clescot.kafka.connect.http.client.proxy;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;

public class WeightedRandomProxySelector extends ProxySelector {

    private final NavigableMap<Integer, Proxy> proxies;
    private final Random random;
    private final int totalWeight;

    public WeightedRandomProxySelector(NavigableMap<Integer,Proxy> proxies, Random random) {
        Preconditions.checkNotNull(proxies,"proxies list must not be null");
        Preconditions.checkArgument(!proxies.isEmpty(),"(weight,proxies) list must not be empty");
        Preconditions.checkNotNull(random,"random must not be null");
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
        //no action is needed when a connection failed
    }
}
