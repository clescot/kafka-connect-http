package io.github.clescot.kafka.connect.http.client.okhttp;

import no.nav.security.mock.oauth2.MockOAuth2Server;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class OAuth2OkHttpClientTest {

    private static MockOAuth2Server server;

    @BeforeAll
    static void setUp(){
        server = new MockOAuth2Server();
        server.start();
    }

    @AfterAll
    static void tearsDown(){
        server.shutdown();
    }

    @Test
    void test_nominal(){

        String issuerId= "default";
        String wellKnownUrl = server.wellKnownUrl(issuerId).toString();
        System.out.println(wellKnownUrl);
    }


}
