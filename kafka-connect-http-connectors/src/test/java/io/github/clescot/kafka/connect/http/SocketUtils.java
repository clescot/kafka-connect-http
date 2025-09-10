package io.github.clescot.kafka.connect.http;

import org.awaitility.Awaitility;

import java.io.IOException;
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


    public static void awaitUntilPortIsOpen(String host,int port,int timeoutInSeconds) {
        given().ignoreExceptions().await().atMost(timeoutInSeconds, TimeUnit.SECONDS).until(() ->available(host,port));
    }

    private static boolean available(String host,int port) {
        Socket s = null;
        try {
            s = new Socket("localhost", port);
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


}
