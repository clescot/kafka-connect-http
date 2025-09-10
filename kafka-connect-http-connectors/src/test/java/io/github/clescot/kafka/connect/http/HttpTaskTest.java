package io.github.clescot.kafka.connect.http;


import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.google.common.collect.Lists;
import io.github.clescot.kafka.connect.Client;
import io.github.clescot.kafka.connect.Configuration;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition;
import io.github.clescot.kafka.connect.http.sink.HttpConnectorConfig;
import okhttp3.Cookie;
import okhttp3.CookieJar;
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

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Enclosed.class)
public class HttpTaskTest {
    public static final String OK = "OK";
    private static final String DUMMY_BODY_TYPE = "STRING";
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

    private String getIP() {
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
            String bodyResponse = "{\"result\":\"pong\"}";
            wireMock
                    .register(WireMock.post("/ping")
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

            HttpRequest httpRequest =  getDummyHttpRequest("http://"+getIP()+":"+wmRuntimeInfo.getHttpPort()+"/ping");
            HttpExchange httpExchange = (HttpExchange) httpTask.call(httpRequest).get();
            assertThat(httpExchange).isNotNull();
            assertThat(httpExchange.getHttpRequest()).isNotNull();
            assertThat(httpExchange.getHttpResponse()).isNotNull();

        }

        @Test
        void test_two_calls() throws ExecutionException, InterruptedException {
            //given
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();
            WireMock wireMock = wmRuntimeInfo.getWireMock();
            String bodyResponse = "{\"result\":\"pong\"}";
            wireMock
                    .register(WireMock.post("/ping")
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
                    .register(WireMock.post("/ping2")
                            .withHeader("Cookie", WireMock.equalTo("cat=tabby"))
                            .willReturn(WireMock.aResponse()
                                    .withHeader("Content-Type", "application/json")
                                    .withHeader("Set-Cookie", "cat=secondCall; Max-Age=86400")
                                    .withBody(bodyResponse)
                                    .withStatus(201)
                                    .withStatusMessage(OK)
                                    .withFixedDelay(1000)
                            )
                    );
            Map<String,String> settings = Maps.newHashMap();
            HttpConfigDefinition httpConfigDefinition = new HttpConfigDefinition(settings);
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(httpConfigDefinition.config(), settings);
            HttpTask<SinkRecord,OkHttpClient, okhttp3.Request,okhttp3.Response> httpTask = new HttpTask<>(httpConnectorConfig,new OkHttpClientFactory());

            HttpRequest httpRequest =  getDummyHttpRequest("http://"+getIP()+":"+wmRuntimeInfo.getHttpPort()+"/ping");
            HttpExchange httpExchange = httpTask.call(httpRequest).get();
            assertThat(httpExchange).isNotNull();
            assertThat(httpExchange.getHttpRequest()).isNotNull();
            assertThat(httpExchange.getHttpResponse()).isNotNull();
            List<Cookie> cookies = getCookies(httpTask, httpRequest);
            assertThat(cookies).hasSize(1);
            Cookie firstCookie = cookies.get(0);
            assertThat(firstCookie.name()).isEqualTo("cat");
            assertThat(firstCookie.value()).isEqualTo("tabby");


            HttpRequest httpRequest2 =  getDummyHttpRequest("http://"+getIP()+":"+wmRuntimeInfo.getHttpPort()+"/ping2");
            HttpExchange httpExchange2 = (HttpExchange) httpTask.call(httpRequest2).get();
            assertThat(httpExchange2).isNotNull();
            List<Cookie> cookies2 = getCookies(httpTask, httpRequest2);
            assertThat(cookies2).hasSize(1);
            Cookie firstCookie2 = cookies2.get(0);
            assertThat(firstCookie2.name()).isEqualTo("cat");
            assertThat(firstCookie2.value()).isEqualTo("secondCall");


        }

    }

    private List<Cookie> getCookies(HttpTask httpTask, HttpRequest httpRequest) {
        OkHttpClient client = (OkHttpClient) httpTask.selectConfiguration(httpRequest).getClient();
        HttpUrl url = client.buildNativeRequest(httpRequest).url();
        return client.getCookieJar().loadForRequest(url);
    }

    @Nested
    class SelectionConfiguration{

        @Test
        void test_when_two_request_have_different_vu_id_then_two_different_configuration_instances_are_selected() {
            //given
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();

            Map<String, String> settings = Maps.newHashMap();
            HttpConfigDefinition httpConfigDefinition = new HttpConfigDefinition(settings);
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(httpConfigDefinition.config(), settings);
            HttpTask httpTask = new HttpTask(httpConnectorConfig, new OkHttpClientFactory());

            HttpRequest httpRequest = getDummyHttpRequest("http://127.0.0.1:" + wmRuntimeInfo.getHttpPort() + "/path2");
            httpRequest.addAttribute(Request.VU_ID,"1");
            Configuration configuration = httpTask.selectConfiguration(httpRequest);

            HttpRequest httpRequest2 = (HttpRequest) httpRequest.clone();
            httpRequest2.addAttribute(Request.VU_ID,"2");
            Configuration configuration2 = httpTask.selectConfiguration(httpRequest2);

            assertThat(configuration).isNotSameAs(configuration2);
            assertThat(configuration).isEqualTo(configuration2);
        }

    }
    static HttpRequest getDummyHttpRequest(String url) {
        HttpRequest httpRequest = new HttpRequest(url, HttpRequest.Method.POST);
        Map<String, List<String>> headers = com.google.common.collect.Maps.newHashMap();
        headers.put("Content-Type", Lists.newArrayList("application/json"));
        httpRequest.setHeaders(headers);
        httpRequest.setBodyAsString("stuff");
        httpRequest.setBodyAsForm(com.google.common.collect.Maps.newHashMap());
        return httpRequest;
    }

}