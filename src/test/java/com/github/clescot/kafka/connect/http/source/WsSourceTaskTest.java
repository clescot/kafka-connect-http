package com.github.clescot.kafka.connect.http.source;

import com.github.clescot.kafka.connect.http.QueueFactory;
import com.github.clescot.kafka.connect.http.sink.config.ConfigConstants;
import com.google.common.collect.Maps;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TransferQueue;

class WsSourceTaskTest {
    private WsSourceTask wsSourceTask;

    @BeforeEach
    public void setup(){
        wsSourceTask = new WsSourceTask();
    }

    @Test
    void test_start_with_null_settings() {
        WsSourceTask wsSourceTask = new WsSourceTask();

        Assertions.assertThrows(NullPointerException.class, () -> wsSourceTask.start(null));
    }
    @Test
    void test_start_with_empty_settings() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> wsSourceTask.start(Maps.newHashMap()));
    }

    @Test
    void test_start_nominal_case() {
        Map<String, String> config = getNominalConfig();
        wsSourceTask.start(config);
    }

    @NotNull
    private static Map<String, String> getNominalConfig() {
        Map<String, String> config =  Maps.newHashMap();
        config.put(ConfigConstants.ACK_TOPIC,"ack-topic");
        return config;
    }

    @Test
    void poll() throws InterruptedException {
        wsSourceTask.start(getNominalConfig());
        TransferQueue<Acknowledgement> queue = QueueFactory.getQueue();
        ExecutorService exService = Executors.newFixedThreadPool(3);

        wsSourceTask.poll();

    }
}