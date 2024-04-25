package io.github.clescot.kafka.connect.http.client.okhttp.authentication;

import com.google.common.collect.Maps;
import okhttp3.Authenticator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BasicAuthenticationConfigurerTest {

    @Test
    void test_configure_authenticator_with_null(){
        BasicAuthenticationConfigurer authenticationConfigurer = new BasicAuthenticationConfigurer();
        Assertions.assertThrows(NullPointerException.class,()->authenticationConfigurer.configureAuthenticator(null),"config map is null");
    }

    @Test
    void test_configure_authenticator_with_empty_map(){
        BasicAuthenticationConfigurer authenticationConfigurer = new BasicAuthenticationConfigurer();
        Authenticator authenticator = authenticationConfigurer.configureAuthenticator(Maps.newHashMap());
        assertThat(authenticator).isNull();
    }

}