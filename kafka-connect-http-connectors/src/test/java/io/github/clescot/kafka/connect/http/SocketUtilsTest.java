package io.github.clescot.kafka.connect.http;

import io.github.clescot.client.http.SocketUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.*;

class SocketUtilsTest {

    @Test
    void testGetRandomAvailablePortReturnsFreePort() {
        int port = SocketUtils.getRandomAvailablePort();

        // Verify the port is NOT "available" (aka listening) according to
        // SocketUtils.available logic
        assertFalse(SocketUtils.available(port), "Port should not be listening");

        // Verify we can actually bind to it
        try (ServerSocket ss = new ServerSocket(port)) {
            // Success
        } catch (IOException e) {
            fail("Should be able to bind to the returned port " + port);
        }
    }

    @Test
    void testAvailableDetectsListeningPort() throws IOException {
        try (ServerSocket ss = new ServerSocket(0)) {
            int port = ss.getLocalPort();
            // It should be "available" (listening)
            boolean available = SocketUtils.available("localhost", port);
            // Note: available() connects to getIP(), so we rely on that.
            // If getIP() != localhost, this might accept connection if bound to 0.0.0.0

            // Let's use the same method as SocketUtils checks: available(port)
            assertTrue(SocketUtils.available(port), "Should detect port is listening");
        }
    }
}
