package io.github.clescot.kafka.connect.http.client.okhttp.authentication;

import com.burgstaller.okhttp.digest.DigestAuthenticator;
import com.google.common.collect.Maps;
import okhttp3.Authenticator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Random;

import static io.github.clescot.kafka.connect.http.sink.HttpConfigDefinition.*;
import static org.assertj.core.api.Assertions.assertThat;

class DigestAuthenticationConfigurerTest {

    @Test
    void test_authentication_scheme(){
        AuthenticationConfigurer authenticationConfigurer = new DigestAuthenticationConfigurer(new Random());
        assertThat(authenticationConfigurer.authenticationScheme()).isEqualTo("digest");
    }

    @Test
    void test_need_cache(){
        AuthenticationConfigurer authenticationConfigurer = new DigestAuthenticationConfigurer(new Random());
        assertThat(authenticationConfigurer.needCache()).isTrue();
    }

    @Test
    void test_configure_authenticator_nominal_case(){
        AuthenticationConfigurer authenticationConfigurer = new DigestAuthenticationConfigurer(new Random());
        Map<String,Object> config = Maps.newHashMap();
        config.put(HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE,Boolean.TRUE);
        config.put(HTTP_CLIENT_AUTHENTICATION_DIGEST_USERNAME,"myUser");
        config.put(HTTP_CLIENT_AUTHENTICATION_DIGEST_PASSWORD,"stuff");
        Authenticator authenticator = authenticationConfigurer.configureAuthenticator(config);
        assertThat(authenticator)
                .isNotNull()
                .isInstanceOf(DigestAuthenticator.class);
    }
    @Test
    void test_configure_authenticator_with_defined_charset(){
        AuthenticationConfigurer authenticationConfigurer = new DigestAuthenticationConfigurer(new Random());
        Map<String,Object> config = Maps.newHashMap();
        config.put(HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE,Boolean.TRUE);
        config.put(HTTP_CLIENT_AUTHENTICATION_DIGEST_USERNAME,"myUser");
        config.put(HTTP_CLIENT_AUTHENTICATION_DIGEST_PASSWORD,"stuff");
        config.put(HTTP_CLIENT_AUTHENTICATION_DIGEST_CHARSET,"UTF-8");
        Authenticator authenticator = authenticationConfigurer.configureAuthenticator(config);
        assertThat(authenticator)
                .isNotNull()
                .isInstanceOf(DigestAuthenticator.class);
    }

    @Test
    void test_configure_authenticator_with_undefined_charset(){
        AuthenticationConfigurer authenticationConfigurer = new DigestAuthenticationConfigurer(new Random());
        Map<String,Object> config = Maps.newHashMap();
        config.put(HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE,Boolean.TRUE);
        config.put(HTTP_CLIENT_AUTHENTICATION_DIGEST_USERNAME,"myUser");
        config.put(HTTP_CLIENT_AUTHENTICATION_DIGEST_PASSWORD,"stuff");
        config.put(HTTP_CLIENT_AUTHENTICATION_DIGEST_CHARSET,"ddd");
        Assertions.assertThrows(java.nio.charset.UnsupportedCharsetException.class,()->authenticationConfigurer.configureAuthenticator(config));
    }

    @Test
    void test_configure_authenticator_missing_username(){
        AuthenticationConfigurer authenticationConfigurer = new DigestAuthenticationConfigurer(new Random());
        Map<String,Object> config = Maps.newHashMap();
        config.put(HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE,Boolean.TRUE);
        config.put(HTTP_CLIENT_AUTHENTICATION_DIGEST_PASSWORD,"stuff");
        Assertions.assertThrows(NullPointerException.class,()->authenticationConfigurer.configureAuthenticator(config));
    }
    @Test
    void test_configure_authenticator_missing_password(){
        AuthenticationConfigurer authenticationConfigurer = new DigestAuthenticationConfigurer(new Random());
        Map<String,Object> config = Maps.newHashMap();
        config.put(HTTP_CLIENT_AUTHENTICATION_DIGEST_ACTIVATE,Boolean.TRUE);
        config.put(HTTP_CLIENT_AUTHENTICATION_DIGEST_USERNAME,"myUser");
        Assertions.assertThrows(NullPointerException.class,()->authenticationConfigurer.configureAuthenticator(config));
    }

    @Test
    void test_configure_with_null_map(){
        AuthenticationConfigurer authenticationConfigurer = new DigestAuthenticationConfigurer(new Random());
        Assertions.assertThrows(NullPointerException.class,()->authenticationConfigurer.configureAuthenticator(null));
    }

    @Test
    void test_configure_with_empty_map(){
        AuthenticationConfigurer authenticationConfigurer = new DigestAuthenticationConfigurer(new Random());
        Map<String,Object> config = Maps.newHashMap();
        Authenticator authenticator = authenticationConfigurer.configureAuthenticator(config);
        assertThat(authenticator).isNull();
    }


}