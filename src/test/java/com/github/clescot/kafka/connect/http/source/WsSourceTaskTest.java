package com.github.clescot.kafka.connect.http.source;

import com.github.clescot.kafka.connect.http.HttpRequest;
import com.github.clescot.kafka.connect.http.QueueFactory;
import com.github.clescot.kafka.connect.http.QueueProducer;
import com.google.common.collect.Maps;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.source.SourceRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.clescot.kafka.connect.http.source.WsSourceConfigDefinition.ERROR_TOPIC;
import static com.github.clescot.kafka.connect.http.source.WsSourceConfigDefinition.SUCCESS_TOPIC;
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
        Queue<HttpExchange> queue = QueueFactory.getQueue();
        queue.clear();
    }

    @Test
    public void test_start_with_null_settings() {
        WsSourceTask wsSourceTask = new WsSourceTask();

        Assertions.assertThrows(NullPointerException.class, () -> wsSourceTask.start(null));
    }

    @Test
    public void test_start_with_empty_settings() {
        Assertions.assertThrows(ConfigException.class, () -> wsSourceTask.start(Maps.newHashMap()));
    }

    @Test
    public void test_start_nominal_case() {
        Map<String, String> config = getNominalConfig();
        wsSourceTask.start(config);
    }

    @NotNull
    private static Map<String, String> getNominalConfig() {
        Map<String, String> config = Maps.newHashMap();
        config.put(SUCCESS_TOPIC, "http-success");
        config.put(ERROR_TOPIC, "http-error");
        return config;
    }

    @Test
    public void poll() throws ExecutionException, InterruptedException {
        wsSourceTask.start(getNominalConfig());
        Queue<HttpExchange> queue = QueueFactory.getQueue();
        ExecutorService exService = Executors.newFixedThreadPool(3);
        long expectedNumberOfSuccessfulMessages = 50L;
        long expectedNumberOfFailureMessages = 30L;
        QueueProducer queueProducer = new QueueProducer(QueueFactory.getQueue(), expectedNumberOfSuccessfulMessages, expectedNumberOfFailureMessages);
        exService.submit(queueProducer).get();

        List<SourceRecord> sourceRecords = wsSourceTask.poll();
        long successfulMessagesCount = sourceRecords.stream().filter(sourceRecord -> sourceRecord.topic().equals(getNominalConfig().get(SUCCESS_TOPIC))).count();
        assertThat(successfulMessagesCount).isEqualTo(expectedNumberOfSuccessfulMessages);
        long errorMessagesCount = sourceRecords.stream().filter(sourceRecord -> sourceRecord.topic().equals(getNominalConfig().get(ERROR_TOPIC))).count();
        assertThat(errorMessagesCount).isEqualTo(expectedNumberOfFailureMessages);
        //we have consumed all messages
        assertThat(queue).isEmpty();
    }
    @Test
    public void test_success()  {
        wsSourceTask.start(getNominalConfig());
        Queue<HttpExchange> queue = QueueFactory.getQueue();
        HttpRequest httpRequest = new HttpRequest(
                "http://www.dummy.com",
                "GET",
                "STRING",
                "stuff",
                null,
                null
                );
        HttpExchange httpExchange = new HttpExchange(
                httpRequest,
                200,
                "OK",
                Maps.newHashMap(),
                "dummy response",
                210,
                OffsetDateTime.now(ZoneId.of("UTC")),
                new AtomicInteger(1),
                true);
        queue.offer(httpExchange);
        List<SourceRecord> sourceRecords = wsSourceTask.poll();
        long successfulMessagesCount = sourceRecords.stream().filter(sourceRecord -> sourceRecord.topic().equals(getNominalConfig().get(SUCCESS_TOPIC))).count();
        assertThat(successfulMessagesCount).isEqualTo(1);
        long errorMessagesCount = sourceRecords.stream().filter(sourceRecord -> sourceRecord.topic().equals(getNominalConfig().get(ERROR_TOPIC))).count();
        assertThat(errorMessagesCount).isEqualTo(0);
        //we have consumed all messages
        assertThat(queue).isEmpty();
    }    @Test
    public void test_error()  {
        wsSourceTask.start(getNominalConfig());
        Queue<HttpExchange> queue = QueueFactory.getQueue();
        HttpRequest httpRequest = new HttpRequest(
                "http://www.dummy.com",
                "GET",
                "STRING",
                "stuff",
                null,
                null
                );
        HttpExchange httpExchange = new HttpExchange(
                httpRequest,
                500,
                "OK",
                Maps.newHashMap(),
                "dummy response",
                210,
                OffsetDateTime.now(ZoneId.of("UTC")),
                new AtomicInteger(1),
                false);
        queue.offer(httpExchange);
        List<SourceRecord> sourceRecords = wsSourceTask.poll();
        long successfulMessagesCount = sourceRecords.stream().filter(sourceRecord -> sourceRecord.topic().equals(getNominalConfig().get(SUCCESS_TOPIC))).count();
        assertThat(successfulMessagesCount).isEqualTo(0);
        long errorMessagesCount = sourceRecords.stream().filter(sourceRecord -> sourceRecord.topic().equals(getNominalConfig().get(ERROR_TOPIC))).count();
        assertThat(errorMessagesCount).isEqualTo(1);
        //we have consumed all messages
        assertThat(queue).isEmpty();
    }

}