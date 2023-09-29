package io.github.clescot.kafka.connect.http.client.proxy;


import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.IOException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class URIRegexProxySelector extends ProxySelector {

    private final List<ImmutablePair<Predicate<URI>, Proxy>> proxies;

    public URIRegexProxySelector(List<ImmutablePair<Predicate<URI>,Proxy>> proxies) {
        Preconditions.checkNotNull(proxies,"proxies list must not be null");
        Preconditions.checkArgument(!proxies.isEmpty(),"proxies list must not be empty");
        this.proxies = proxies;
    }

    @Override
    public List<Proxy> select(URI uri) {
        return proxies.stream().filter(pair -> pair.getLeft().test(uri)).map(ImmutablePair::getRight).collect(Collectors.toList());
    }

    @Override
    public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {

    }
}
