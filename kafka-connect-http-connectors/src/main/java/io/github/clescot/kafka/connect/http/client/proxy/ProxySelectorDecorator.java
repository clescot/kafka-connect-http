package io.github.clescot.kafka.connect.http.client.proxy;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.util.List;
import java.util.regex.Pattern;

import static java.net.Proxy.NO_PROXY;

public class ProxySelectorDecorator extends ProxySelector {

    private final ProxySelector proxySelector;
    private final Pattern nonProxyHostsUriPattern;

    public ProxySelectorDecorator(ProxySelector proxySelector,
                                  Pattern nonProxyHostsUriPattern) {
        Preconditions.checkNotNull(proxySelector,"proxySelector must not be null");
        Preconditions.checkNotNull(nonProxyHostsUriPattern,"nonProxyHostsUriPattern must not be null");
        this.proxySelector = proxySelector;
        this.nonProxyHostsUriPattern = nonProxyHostsUriPattern;
    }

    @Override
    public List<Proxy> select(URI uri) {
        if(nonProxyHostsUriPattern.matcher(uri.toString()).matches()){
            return List.of(NO_PROXY);
        }
        return proxySelector.select(uri);
    }

    @Override
    public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {
        proxySelector.connectFailed(uri,sa,ioe);
    }
}
