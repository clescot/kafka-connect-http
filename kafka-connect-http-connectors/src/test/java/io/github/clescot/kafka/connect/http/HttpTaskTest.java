package io.github.clescot.kafka.connect.http;


import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.google.common.collect.Lists;
import io.github.clescot.kafka.connect.Configuration;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition;
import io.github.clescot.kafka.connect.http.sink.HttpConnectorConfig;
import org.junit.experimental.runners.Enclosed;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.runner.RunWith;
import org.testcontainers.shaded.com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Enclosed.class)
public class HttpTaskTest {

    private static final HttpRequest.Method DUMMY_METHOD = HttpRequest.Method.POST;
    private static final String DUMMY_BODY = "stuff";
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

    @Nested
    class Call {

        @Test
        void test_nominal_case() throws ExecutionException, InterruptedException {
            //given
            WireMockRuntimeInfo wmRuntimeInfo = wmHttp.getRuntimeInfo();

            Map<String,String> settings = Maps.newHashMap();
            HttpConfigDefinition httpConfigDefinition = new HttpConfigDefinition(settings);
            HttpConnectorConfig httpConnectorConfig = new HttpConnectorConfig(httpConfigDefinition.config(), settings);
            HttpTask httpTask = new HttpTask(httpConnectorConfig,new OkHttpClientFactory());

            HttpRequest httpRequest =  getDummyHttpRequest("http://127.0.0.1:"+wmRuntimeInfo.getHttpPort()+"/path2");
            HttpExchange httpExchange = (HttpExchange) httpTask.call(httpRequest).get();
            assertThat(httpExchange).isNotNull();
            assertThat(httpExchange.getHttpRequest()).isNotNull();
            assertThat(httpExchange.getHttpResponse()).isNotNull();

        }

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
        HttpRequest httpRequest = new HttpRequest(url, DUMMY_METHOD);
        Map<String, List<String>> headers = com.google.common.collect.Maps.newHashMap();
        headers.put("Content-Type", Lists.newArrayList("application/json"));
        httpRequest.setHeaders(headers);
        httpRequest.setBodyAsString("stuff");
        httpRequest.setBodyAsForm(com.google.common.collect.Maps.newHashMap());
        return httpRequest;
    }


    private String getLocalHttpRequestAsStringWithPath(int port, String path, String method, String dummyBody) {
        return "{\n" +
                "  \"url\": \"" + "http://localhost:" + port + path + "\",\n" +
                "  \"headers\": {},\n" +
                "  \"method\": \"" + method + "\",\n" +
                "  \"bodyAsString\": \"" + dummyBody + "\",\n" +
                "  \"bodyType\": \"" + DUMMY_BODY_TYPE + "\"\n" +
                "}";
    }
}