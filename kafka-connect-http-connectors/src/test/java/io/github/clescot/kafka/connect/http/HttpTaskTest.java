package io.github.clescot.kafka.connect.http;


import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.trafficlistener.ConsoleNotifyingWiremockNetworkTrafficListener;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.google.common.collect.Lists;
import de.sstoehr.harreader.model.Har;
import io.github.clescot.kafka.connect.Configuration;
import io.github.clescot.kafka.connect.http.client.HttpConfiguration;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition;
import io.github.clescot.kafka.connect.http.sink.HttpConnectorConfig;
import okhttp3.Cookie;
import okhttp3.HttpUrl;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.experimental.runners.Enclosed;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.runner.RunWith;
import org.testcontainers.shaded.com.google.common.collect.Maps;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static io.github.clescot.kafka.connect.http.SocketUtils.awaitUntilPortIsOpen;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Enclosed.class)
public class HttpTaskTest {
    public static final String OK = "OK";

    private static final String IP = getIP();

    @RegisterExtension
    WireMockExtension wmHttp = WireMockExtension.newInstance()
            .options(
                    WireMockConfiguration.wireMockConfig()
                            .dynamicPort()
                            .networkTrafficListener(new ConsoleNotifyingWiremockNetworkTrafficListener())
                            .useChunkedTransferEncoding(Options.ChunkedEncodingPolicy.NEVER)
            )
            .build();

    @AfterEach
    void tearsDown() {
        wmHttp.resetMappings();
        wmHttp.resetRequests();
        HttpTask.removeCompositeMeterRegistry();
    }

    private static String getIP() {
        try (DatagramSocket datagramSocket = new DatagramSocket()) {
            datagramSocket.connect(InetAddress.getByName("8.8.8.8"), 12345);
            return datagramSocket.getLocalAddress().getHostAddress();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Nested
    class Call {


        @Test
        void test_nominal_case() throws ExecutionException, InterruptedException {
            //given
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            int httpPort = wmRuntimeInfo.getHttpPort();
            awaitUntilPortIsOpen(IP,httpPort,10);
            String bodyResponse = "{\"result\":\"pong\"}";
            wireMock
                    .register(WireMock.post("/ping1")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withHeader("Set-Cookie", "cat=tabby; Max-Age=86400")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage(OK)
                                    .withFixedDelay(1000)
                            )
                    );
            Map<String,String> settings = Maps.newHashMap();
            HttpConfigDefinition httpConfigDefinition = new HttpConfigDefinition(settings);
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(httpConfigDefinition.config(), settings);
            HttpTask httpTask = new HttpTask(httpConnectorConfig,new OkHttpClientFactory());

            HttpRequest httpRequest =  getDummyHttpRequest("http://"+IP+":"+ httpPort +"/ping1","1");
            HttpExchange httpExchange = (HttpExchange) httpTask.call(httpRequest).get();
            assertThat(httpExchange).isNotNull();
            assertThat(httpExchange.getHttpRequest()).isNotNull();
            assertThat(httpExchange.getHttpResponse()).isNotNull();
            Har har = HttpExchange.toHar(httpExchange);
            assertThat(har).isNotNull();
        }

        @Test
        void test_two_calls() throws ExecutionException, InterruptedException {
            //given
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            int httpPort = wmRuntimeInfo.getHttpPort();

            String bodyResponse = "{\"result\":\"pong\"}";
            wireMock
                    .register(WireMock.post("/ping2")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withHeader("Set-Cookie", "cat=tabby; Max-Age=86400")
                                    .withBody(bodyResponse)
                                    .withStatus(200)
                                    .withStatusMessage(OK)
                                    .withFixedDelay(1000)
                            )
                    );
            wireMock
                    .register(WireMock.post("/ping3")
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withHeader("Set-Cookie", "cat=secondCall; Max-Age=86400")
                                    .withBody(bodyResponse)
                                    .withStatus(201)
                                    .withStatusMessage(OK)
                                    .withFixedDelay(1000)
                            )
                    );
            awaitUntilPortIsOpen(IP,httpPort,10);
            Map<String,String> settings = Maps.newHashMap();
            HttpConfigDefinition httpConfigDefinition = new HttpConfigDefinition(settings);
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(httpConfigDefinition.config(), settings);
            HttpTask<SinkRecord,OkHttpClient, okhttp3.Request,okhttp3.Response> httpTask = new HttpTask<>(httpConnectorConfig,new OkHttpClientFactory());


            String vuid = "2";
            HttpRequest httpRequest =  getDummyHttpRequest("http://"+ IP +":"+ httpPort +"/ping2", vuid);
            HttpExchange httpExchange = httpTask.call(httpRequest).get();
            assertThat(httpExchange).isNotNull();
            assertThat(httpExchange.getHttpRequest()).isNotNull();
            assertThat(httpExchange.getHttpResponse()).isNotNull();
            List<Cookie> cookies = getCookies(httpTask, httpRequest);
            assertThat(cookies).hasSize(1);
            Cookie firstCookie = cookies.get(0);
            assertThat(firstCookie.name()).isEqualTo("cat");
            assertThat(firstCookie.value()).isEqualTo("tabby");


            HttpRequest httpRequest2 =  getDummyHttpRequest("http://"+IP+":"+ httpPort +"/ping3", vuid);
            HttpExchange httpExchange2 = httpTask.call(httpRequest2).get();
            assertThat(httpExchange2).isNotNull();
            List<Cookie> cookies2 = getCookies(httpTask, httpRequest2);
            assertThat(cookies2).hasSize(1);
            Cookie firstCookie2 = cookies2.get(0);
            assertThat(firstCookie2.name()).isEqualTo("cat");
            assertThat(firstCookie2.value()).isEqualTo("secondCall");


        }

    }

    private List<Cookie> getCookies(HttpTask httpTask, HttpRequest httpRequest) {
        HttpConfiguration configuration =  (HttpConfiguration) httpTask.selectConfiguration(httpRequest);
        OkHttpClient client = (OkHttpClient) configuration.getClient();
        HttpUrl url = client.buildNativeRequest(httpRequest).url();
        return client.getCookieJar().loadForRequest(url);
    }

    @Nested
    class SelectionConfiguration{
        @RegisterExtension
        static WireMockExtension wmHttp = WireMockExtension.newInstance()
                .options(
                        WireMockConfiguration.wireMockConfig()
                                .dynamicPort()
                )
                .build();

        @AfterEach
        void tearsDown() {
            wmHttp.resetAll();
            HttpTask.removeCompositeMeterRegistry();
        }
        @Test
        void test_when_two_request_have_different_vu_id_then_two_different_configuration_instances_are_selected() {
            //given
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();

            Map<String, String> settings = Maps.newHashMap();
            HttpConfigDefinition httpConfigDefinition = new HttpConfigDefinition(settings);
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(httpConfigDefinition.config(), settings);
            HttpTask httpTask = new HttpTask(httpConnectorConfig, new OkHttpClientFactory());

            HttpRequest httpRequest = getDummyHttpRequest("http://"+IP+":" + wmRuntimeInfo.getHttpPort() + "/path2","3");
            httpRequest.addAttribute(Request.VU_ID,"1");
            Configuration configuration = httpTask.selectConfiguration(httpRequest);

            HttpRequest httpRequest2 = (HttpRequest) httpRequest.clone();
            httpRequest2.addAttribute(Request.VU_ID,"2");
            Configuration configuration2 = httpTask.selectConfiguration(httpRequest2);

            assertThat(configuration).isNotSameAs(configuration2);
            assertThat(configuration).isEqualTo(configuration2);
        }

    }
    static HttpRequest getDummyHttpRequest(String url,String vuId) {
        HttpRequest httpRequest = new HttpRequest(url, HttpRequest.Method.POST);
        Map<String, List<String>> headers = com.google.common.collect.Maps.newHashMap();
        headers.put("Content-Type", Lists.newArrayList("application/json"));
        httpRequest.setHeaders(headers);
        httpRequest.addAttribute(Request.VU_ID,vuId);
        httpRequest.setBodyAsString("stuff");
        httpRequest.setBodyAsForm(com.google.common.collect.Maps.newHashMap());
        return httpRequest;
    }

}