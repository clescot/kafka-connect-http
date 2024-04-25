package io.github.clescot.kafka.connect.http.client.okhttp.authentication;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OAuth2ClientCredentialsFlowAuthenticatorTest {


    @Test
    void test_constructor_with_null_args() {
        Assertions.assertThrows(NullPointerException.class, () -> new OAuth2ClientCredentialsFlowAuthenticator(
                null, null, null, null, null));
    }

}