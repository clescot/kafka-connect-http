package io.github.clescot.kafka.connect.http.client.okhttp;

import no.nav.security.mock.oauth2.MockOAuth2Server;
import no.nav.security.mock.oauth2.token.DefaultOAuth2TokenCallback;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

public class OAuth2Test {
    private static MockOAuth2Server mockOAuth2Server;
    @BeforeAll
    static void beforeAll() throws IOException {
        mockOAuth2Server = new MockOAuth2Server();
        mockOAuth2Server.start(InetAddress.getLocalHost(),findRandomOpenPortOnAllLocalInterfaces());
        mockOAuth2Server.enqueueCallback(new DefaultOAuth2TokenCallback("issuer1", "foo"));
    }


    @Test
    void test_nominal_case(){
        System.out.println("hello");
    }


    private static Integer findRandomOpenPortOnAllLocalInterfaces() throws IOException {
        try (
                ServerSocket socket = new ServerSocket(0);
        ) {
            return socket.getLocalPort();

        }
    }
}
