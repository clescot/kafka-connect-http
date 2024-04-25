package io.github.clescot.kafka.connect.http.client.okhttp.authentication;

import com.burgstaller.okhttp.basic.BasicAuthenticator;
import com.google.common.collect.Maps;
import okhttp3.Authenticator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkConfigDefinition.*;
import static org.assertj.core.api.Assertions.assertThat;

class BasicAuthenticationConfigurerTest {

    @Test
    void test_configure_authenticator_with_null(){
        AuthenticationConfigurer authenticationConfigurer = new BasicAuthenticationConfigurer();
        Assertions.assertThrows(NullPointerException.class,()->authenticationConfigurer.configureAuthenticator(null),"config map is null");
    }

    @Test
    void test_configure_authenticator_with_empty_map(){
        AuthenticationConfigurer authenticationConfigurer = new BasicAuthenticationConfigurer();
        Authenticator authenticator = authenticationConfigurer.configureAuthenticator(Maps.newHashMap());
        assertThat(authenticator).isNull();
    }

    @Test
    void test_configure_authenticator_with_nominal_case(){
        AuthenticationConfigurer authenticationConfigurer = new BasicAuthenticationConfigurer();
        HashMap<String, Object> config = Maps.newHashMap();
        config.put(HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE,Boolean.TRUE);
        config.put(HTTP_CLIENT_AUTHENTICATION_BASIC_USERNAME,"myUser");
        config.put(HTTP_CLIENT_AUTHENTICATION_BASIC_PASSWORD,"myPassword");
        Authenticator authenticator = authenticationConfigurer.configureAuthenticator(config);
        assertThat(authenticator).isNotNull();
        assertThat(authenticator).isInstanceOf(BasicAuthenticator.class);
    }

    @Test
    void test_configure_authenticator_with_defined_charset(){
        AuthenticationConfigurer authenticationConfigurer = new BasicAuthenticationConfigurer();
        HashMap<String, Object> config = Maps.newHashMap();
        config.put(HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE,Boolean.TRUE);
        config.put(HTTP_CLIENT_AUTHENTICATION_BASIC_USERNAME,"myUser");
        config.put(HTTP_CLIENT_AUTHENTICATION_BASIC_PASSWORD,"myPassword");
        config.put(HTTP_CLIENT_AUTHENTICATION_BASIC_CHARSET,"UTF-8");
        Authenticator authenticator = authenticationConfigurer.configureAuthenticator(config);
        assertThat(authenticator).isNotNull();
        assertThat(authenticator).isInstanceOf(BasicAuthenticator.class);
    }

    @Test
    void test_configure_authenticator_with_unknown_charset(){
        AuthenticationConfigurer authenticationConfigurer = new BasicAuthenticationConfigurer();
        HashMap<String, Object> config = Maps.newHashMap();
        config.put(HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE,Boolean.TRUE);
        config.put(HTTP_CLIENT_AUTHENTICATION_BASIC_USERNAME,"myUser");
        config.put(HTTP_CLIENT_AUTHENTICATION_BASIC_PASSWORD,"myPassword");
        config.put(HTTP_CLIENT_AUTHENTICATION_BASIC_CHARSET,"ddqd");
        Assertions.assertThrows(java.nio.charset.UnsupportedCharsetException.class,()->authenticationConfigurer.configureAuthenticator(config));
    }

    @Test
    void test_configure_authenticator_with_missing_username(){
        AuthenticationConfigurer authenticationConfigurer = new BasicAuthenticationConfigurer();
        HashMap<String, Object> config = Maps.newHashMap();
        config.put(HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE,Boolean.TRUE);
        config.put(HTTP_CLIENT_AUTHENTICATION_BASIC_PASSWORD,"myPassword");
        Assertions.assertThrows(NullPointerException.class,()->authenticationConfigurer.configureAuthenticator(config));
    }

    @Test
    void test_configure_authenticator_with_missing_password(){
        AuthenticationConfigurer authenticationConfigurer = new BasicAuthenticationConfigurer();
        HashMap<String, Object> config = Maps.newHashMap();
        config.put(HTTP_CLIENT_AUTHENTICATION_BASIC_ACTIVATE,Boolean.TRUE);
        config.put(HTTP_CLIENT_AUTHENTICATION_BASIC_USERNAME,"myUser");
        Assertions.assertThrows(NullPointerException.class,()->authenticationConfigurer.configureAuthenticator(config));
    }

    @Test
    void test_authentication_scheme(){
        AuthenticationConfigurer authenticationConfigurer = new BasicAuthenticationConfigurer();
        assertThat(authenticationConfigurer.authenticationScheme()).isEqualTo("basic");
    }

    @Test
    void test_need_cache(){
        AuthenticationConfigurer authenticationConfigurer = new BasicAuthenticationConfigurer();
        assertThat(authenticationConfigurer.needCache()).isTrue();
    }

}