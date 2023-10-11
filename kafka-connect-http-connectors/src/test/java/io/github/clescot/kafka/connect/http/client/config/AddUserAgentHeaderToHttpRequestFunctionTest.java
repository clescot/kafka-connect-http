package io.github.clescot.kafka.connect.http.client.config;

import com.google.common.collect.Lists;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.github.clescot.kafka.connect.http.client.config.AddUserAgentHeaderToHttpRequestFunction.USER_AGENT;
import static org.assertj.core.api.Assertions.assertThat;

class AddUserAgentHeaderToHttpRequestFunctionTest {


    @Test
    void test_undefined_user_agent() {
        //given
        List<String> userAgents = Lists.newArrayList("1");
        AddUserAgentHeaderToHttpRequestFunction function = new AddUserAgentHeaderToHttpRequestFunction(userAgents,new Random());
        HttpRequest  httpRequest = new HttpRequest("http://test.com");
        //when
        HttpRequest modifiedRequest = function.apply(httpRequest);
        //then
        assertThat(modifiedRequest.getHeaders().get(USER_AGENT)).hasSize(1);
        assertThat(modifiedRequest.getHeaders().get(USER_AGENT)).isEqualTo(userAgents);
    }

    @Test
    void test_multiple_user_agent() {
        //given
        List<String> userAgents = Lists.newArrayList("1", "2", "3");
        AddUserAgentHeaderToHttpRequestFunction function = new AddUserAgentHeaderToHttpRequestFunction(userAgents,new Random());
        HttpRequest  httpRequest = new HttpRequest("http://test.com");
        //when
        HttpRequest modifiedRequest = function.apply(httpRequest);
        //then
        assertThat(modifiedRequest.getHeaders().get(USER_AGENT)).isNotEmpty();
        assertThat(modifiedRequest.getHeaders().get(USER_AGENT)).isSubsetOf(userAgents);
    }

    @Test
    void test_null_user_agent() {
        //given
        Random random = new Random();
        //when
        Assertions.assertThrows(NullPointerException.class,()->new AddUserAgentHeaderToHttpRequestFunction(null, random));
    }
    @Test
    void test_empty_user_agent() {
        //given
        Random random = new Random();
        ArrayList<String> userAgents = Lists.newArrayList();
        //when
        Assertions.assertThrows(IllegalArgumentException.class,()->new AddUserAgentHeaderToHttpRequestFunction(userAgents,random));
    }
    @Test
    void test_null_random() {
        //when
        List<String> userAgents = Lists.newArrayList("test");
        Assertions.assertThrows(NullPointerException.class,()-> {
            new AddUserAgentHeaderToHttpRequestFunction(userAgents,null);
        });
    }

    @Test
    void test_already_defined_user_agent() {
        //given
        AddUserAgentHeaderToHttpRequestFunction function = new AddUserAgentHeaderToHttpRequestFunction(Lists.newArrayList("test"),new Random());
        HttpRequest  httpRequest = new HttpRequest("http://test.com");
        String initialUserAgent = "initial";
        httpRequest.getHeaders().put(USER_AGENT, Lists.newArrayList(initialUserAgent));
        //when
        HttpRequest modifiedRequest = function.apply(httpRequest);
        //then
        assertThat(modifiedRequest.getHeaders().get(USER_AGENT)).isNotEmpty();
        assertThat(modifiedRequest.getHeaders().get(USER_AGENT).get(0)).isEqualTo(initialUserAgent);
    }
}