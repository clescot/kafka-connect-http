package com.github.clescot.kafka.connect.http.source;

import com.github.clescot.kafka.connect.http.QueueFactory;
import com.github.clescot.kafka.connect.http.QueueProducer;
import com.github.clescot.kafka.connect.http.sink.config.ConfigConstants;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.source.SourceRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
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

import static org.assertj.core.api.Assertions.assertThat;

class WsSourceTaskTest {
    private WsSourceTask wsSourceTask;
    private final static Logger LOGGER = LoggerFactory.getLogger(WsSourceTaskTest.class);

    @BeforeEach
    public void setup() {
        wsSourceTask = new WsSourceTask();
    }
    @AfterEach
    public void tearsDown() {
        Queue<Acknowledgement> queue = QueueFactory.getQueue();
        queue.clear();
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
        Map<String, String> config = Maps.newHashMap();
        config.put(ConfigConstants.SUCCESS_TOPIC, "http-success");
        config.put(ConfigConstants.ERRORS_TOPIC, "http-errors");
        return config;
    }

    @Test
    void poll() throws ExecutionException, InterruptedException {
        wsSourceTask.start(getNominalConfig());
        Queue<Acknowledgement> queue = QueueFactory.getQueue();
        ExecutorService exService = Executors.newFixedThreadPool(3);
        long expectedNumberOfSuccessfulMessages = 50L;
        long expectedNumberOfFailureMessages = 30L;
        QueueProducer queueProducer = new QueueProducer(QueueFactory.getQueue(), expectedNumberOfSuccessfulMessages, expectedNumberOfFailureMessages);
        exService.submit(queueProducer).get();

        List<SourceRecord> sourceRecords = wsSourceTask.poll();
        long successfulMessagesCount = sourceRecords.stream().filter(sourceRecord -> sourceRecord.topic().equals(getNominalConfig().get(ConfigConstants.SUCCESS_TOPIC))).count();
        assertThat(successfulMessagesCount).isEqualTo(expectedNumberOfSuccessfulMessages);
        long errorMessagesCount = sourceRecords.stream().filter(sourceRecord -> sourceRecord.topic().equals(getNominalConfig().get(ConfigConstants.ERRORS_TOPIC))).count();
        assertThat(errorMessagesCount).isEqualTo(expectedNumberOfFailureMessages);
        //we have consumed all messages
        assertThat(queue).isEmpty();


    }
}