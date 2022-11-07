package com.github.clescot.kafka.connect.http.source;

import com.github.clescot.kafka.connect.http.QueueFactory;
import com.github.clescot.kafka.connect.http.QueueProducer;
import com.github.clescot.kafka.connect.http.sink.config.ConfigConstants;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.source.SourceRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class WsSourceTaskTest {
    private WsSourceTask wsSourceTask;
    private final static Logger LOGGER = LoggerFactory.getLogger(WsSourceTaskTest.class);

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
        config.put(ConfigConstants.SUCCESS_TOPIC,"ack-topic");
        return config;
    }

    @Test
    void poll() throws ExecutionException, InterruptedException {
        wsSourceTask.start(getNominalConfig());
        Queue<Acknowledgement> queue = QueueFactory.getQueue();
        ExecutorService exService = Executors.newFixedThreadPool(3);
        int numberOfMessagesToProduce = 50;
        QueueProducer queueProducer = new QueueProducer(QueueFactory.getQueue(), numberOfMessagesToProduce);
        queueProducer.run();
        exService.submit(queueProducer).get();
        for (int i = 0; i < numberOfMessagesToProduce; i++) {
            List<SourceRecord> sourceRecords = wsSourceTask.poll();
            for (SourceRecord sourceRecord : sourceRecords) {
                LOGGER.debug("sourceRecord:{}",sourceRecord);
            }
        }


    }
}