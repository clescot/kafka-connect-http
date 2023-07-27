package io.github.clescot.kafka.connect.http.sink.client.proxy;

import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.trafficlistener.ConsoleNotifyingWiremockNetworkTrafficListener;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.net.*;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;
import static org.assertj.core.api.Assertions.assertThat;

class ProxySelectorFactoryTest {

    @RegisterExtension
    static WireMockExtension wmHttp;

    static {

        wmHttp = WireMockExtension.newInstance()
                .options(
                        WireMockConfiguration.wireMockConfig()
                                .dynamicPort()
                                .networkTrafficListener(new ConsoleNotifyingWiremockNetworkTrafficListener())
                                .useChunkedTransferEncoding(Options.ChunkedEncodingPolicy.NEVER)
                )
                .build();
    }
    @Nested
    class TestBuild {
        @Test
        public void test_null_arguments() {
            ProxySelectorFactory proxySelectorFactory = new ProxySelectorFactory();
            Assertions.assertThrows(NullPointerException.class,()->proxySelectorFactory.build(null,null));
        }

        @Test
        public void test_empty_config_map() {
            ProxySelectorFactory proxySelectorFactory = new ProxySelectorFactory();
            Assertions.assertThrows(IllegalArgumentException.class,()->proxySelectorFactory.build(Maps.newHashMap(), new Random()));
        }

        @Test
        public void test_proxyselector_algorithm_set_to_uriregex_with_hostname_but_no_regex() {
            ProxySelectorFactory proxySelectorFactory = new ProxySelectorFactory();
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(PROXY_SELECTOR_ALGORITHM,"uriregex");
            config.put(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX +"0.hostname","http://dummy\\.com:9999");
            Assertions.assertThrows(NullPointerException.class,()->proxySelectorFactory.build(config, new Random()));
        }
        @Test
        public void test_proxyselector_algorithm_set_to_uriregex_with_hostname_and_regex() {
            ProxySelectorFactory proxySelectorFactory = new ProxySelectorFactory();
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(PROXY_SELECTOR_ALGORITHM,"uriregex");
            config.put(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX +"0.hostname",getIP());
            config.put(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX +"0.port",wmHttp.getPort());
            config.put(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX +"0.type", "HTTP");
            config.put(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX +"0.uri.regex","http://dummy\\.com.*");
            ProxySelector proxySelector = proxySelectorFactory.build(config, new Random());
            assertThat(URIRegexProxySelector.class).isAssignableFrom(proxySelector.getClass());
        }
        @Test
        public void test_proxyselector_algorithm_set_to_uriregex_with_hostname_and_regex_with_non_proxy_hosts() throws URISyntaxException {
            ProxySelectorFactory proxySelectorFactory = new ProxySelectorFactory();
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(PROXY_SELECTOR_ALGORITHM,"uriregex");
            config.put(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX +"0.hostname",getIP());
            config.put(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX +"0.port",wmHttp.getPort());
            config.put(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX +"0.type", "HTTP");
            config.put(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX +"0.uri.regex","http://dummy\\.com.*");
            config.put(PROXY_SELECTOR_HTTP_CLIENT_NON_PROXY_HOSTS_URI_REGEX,"http(s?):\\/\\/test\\.net.*");
            ProxySelector proxySelector = proxySelectorFactory.build(config, new Random());
            assertThat(ProxySelectorDecorator.class).isAssignableFrom(proxySelector.getClass());

            //proxy url
            List<Proxy> proxies = proxySelector.select(new URI("http://dummy.com/ping"));
            assertThat(proxies).hasSize(1);
            Proxy proxy = proxies.get(0);
            assertThat(proxy.type()).isEqualTo(Proxy.Type.HTTP);
            assertThat(proxy.address().toString()).isEqualTo("/"+getIP()+":"+wmHttp.getPort());

            //non proxy url
            List<Proxy> proxies2 = proxySelector.select(new URI("https://test.net/stuff"));
            assertThat(proxies2).hasSize(1);
            Proxy proxy2 = proxies2.get(0);
            assertThat(proxy2).isEqualTo(Proxy.NO_PROXY);
        }

        @Test
        public void test_proxyselector_algorithm_set_to_weightedrandom() {
            ProxySelectorFactory proxySelectorFactory = new ProxySelectorFactory();
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(PROXY_SELECTOR_ALGORITHM,"weightedrandom");
            config.put(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX +"0.hostname",getIP());
            config.put(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX +"0.port",wmHttp.getPort());
            config.put(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX +"0.weight",20);
            ProxySelector proxySelector = proxySelectorFactory.build(config, new Random());
            assertThat(WeightedRandomProxySelector.class).isAssignableFrom(proxySelector.getClass());
        }

        @Test
        public void test_proxyselector_algorithm_set_to_hosthash() {
            ProxySelectorFactory proxySelectorFactory = new ProxySelectorFactory();
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(PROXY_SELECTOR_ALGORITHM,"hosthash");
            config.put(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX +"0.hostname",getIP());
            config.put(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX +"0.port",wmHttp.getPort());
            ProxySelector proxySelector = proxySelectorFactory.build(config, new Random());
            assertThat(HostHashProxySelector.class).isAssignableFrom(proxySelector.getClass());
        }

        @Test
        public void test_proxyselector_algorithm_set_to_random() {
            ProxySelectorFactory proxySelectorFactory = new ProxySelectorFactory();
            HashMap<String, Object> config = Maps.newHashMap();
            config.put(PROXY_SELECTOR_ALGORITHM,"random");
            config.put(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX +"0.hostname",getIP());
            config.put(PROXYSELECTOR_PREFIX + HTTP_CLIENT_PREFIX +"0.port",wmHttp.getPort());
            ProxySelector proxySelector = proxySelectorFactory.build(config, new Random());
            assertThat(RandomProxySelector.class).isAssignableFrom(proxySelector.getClass());
        }
    }
    private String getIP() {
        try (DatagramSocket datagramSocket = new DatagramSocket()) {
            datagramSocket.connect(InetAddress.getByName("8.8.8.8"), 12345);
            return datagramSocket.getLocalAddress().getHostAddress();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}