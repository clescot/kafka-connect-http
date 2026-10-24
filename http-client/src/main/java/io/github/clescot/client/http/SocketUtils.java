package io.github.clescot.client.http;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.given;

public class SocketUtils {

    private static final Random random = new Random();

    private SocketUtils(){}

    public static int getRandomPort() {
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
        try (Socket socket = new Socket(host, port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public static boolean available(int port) {
        return available(getIP(), port);
    }

    public static String getIP() {
        try (DatagramSocket datagramSocket = new DatagramSocket()) {
            // the remote address is only used to determine the local address of the default
            // route: 'connect' on a DatagramSocket does not send any data. the loopback address
            // is used, so that no external address is hardcoded and no network access is needed.
            datagramSocket.connect(InetAddress.getLoopbackAddress(), 12345);
            return datagramSocket.getLocalAddress().getHostAddress();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
