package io.github.clescot.kafka.connect.http;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.given;

public class SocketUtils {
    public static int getRandomPort() {
        Random random = new Random();
        int low = 49152;
        int high = 65535;
        return random.nextInt(high - low) + low;
    }

    public static int getRandomAvailablePort() {
        int randomPort = getRandomPort();
        while (available(randomPort)) {
            randomPort = getRandomPort();
        }
        return randomPort;
    }

    public static void awaitUntilPortIsOpen(String host, int port, int timeoutInSeconds) {
        given().ignoreExceptions().await().atMost(timeoutInSeconds, TimeUnit.SECONDS)
                .until(() -> available(host, port));
    }

    public static boolean available(String host, int port) {
        Socket s = null;
        try {
            s = new Socket(host, port);
            return true;
        } catch (IOException e) {
            return false;
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage());
                }
            }
        }
    }

    public static boolean available(int port) {
        return available(getIP(), port);
    }

    public static String getIP() {
        try (DatagramSocket datagramSocket = new DatagramSocket()) {
            datagramSocket.connect(InetAddress.getByName("8.8.8.8"), 12345);
            return datagramSocket.getLocalAddress().getHostAddress();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
