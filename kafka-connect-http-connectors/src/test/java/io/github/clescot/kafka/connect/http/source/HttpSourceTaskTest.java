package io.github.clescot.kafka.connect.http.source;

import com.google.common.collect.Maps;
import io.github.clescot.kafka.connect.http.QueueProducer;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.core.queue.KafkaRecord;
import io.github.clescot.kafka.connect.http.core.queue.QueueFactory;
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

import static io.github.clescot.kafka.connect.http.source.HttpSourceConfigDefinition.ERROR_TOPIC;
import static io.github.clescot.kafka.connect.http.source.HttpSourceConfigDefinition.SUCCESS_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

class HttpSourceTaskTest {
    private HttpSourceTask wsSourceTask;
    private final static Logger LOGGER = LoggerFactory.getLogger(HttpSourceTaskTest.class);

    @BeforeEach
    public void setup() {
        wsSourceTask = new HttpSourceTask();
    }

    @AfterEach
    public void tearsDown() {
        Queue<KafkaRecord> queue = QueueFactory.getQueue();
        queue.clear();
    }

    @Test
    public void test_start_with_null_settings() {
        HttpSourceTask wsSourceTask = new HttpSourceTask();

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
        Queue<KafkaRecord> queue = QueueFactory.getQueue();
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
    public void test_success() {
        wsSourceTask.start(getNominalConfig());
        Queue<KafkaRecord> queue = QueueFactory.getQueue();
        HttpRequest httpRequest = new HttpRequest(
                "http://www.dummy.com",
                "GET",
                "STRING"
        );
        httpRequest.setBodyAsString("stuff");
        HttpResponse httpResponse = new HttpResponse(200, "OK");
        httpResponse.setResponseBody("dummy response");
        HttpExchange httpExchange = new HttpExchange(
                httpRequest,
                httpResponse,
                210,
                OffsetDateTime.now(ZoneId.of("UTC")),
                new AtomicInteger(1),
                true);
        queue.offer(QueueProducer.toKafkaRecord(httpExchange));
        List<SourceRecord> sourceRecords = wsSourceTask.poll();
        long successfulMessagesCount = sourceRecords.stream().filter(sourceRecord -> sourceRecord.topic().equals(getNominalConfig().get(SUCCESS_TOPIC))).count();
        assertThat(successfulMessagesCount).isEqualTo(1);
        long errorMessagesCount = sourceRecords.stream().filter(sourceRecord -> sourceRecord.topic().equals(getNominalConfig().get(ERROR_TOPIC))).count();
        assertThat(errorMessagesCount).isEqualTo(0);
        //we have consumed all messages
        assertThat(queue).isEmpty();
    }

    @Test
    public void test_error() {
        wsSourceTask.start(getNominalConfig());
        Queue<KafkaRecord> queue = QueueFactory.getQueue();
        HttpRequest httpRequest = new HttpRequest(
                "http://www.dummy.com",
                "GET",
                "STRING"
        );
        httpRequest.setBodyAsString("stuff");
        HttpResponse httpResponse = new HttpResponse(500, "Internal Server Error");
        httpResponse.setResponseBody("dummy response");
        HttpExchange httpExchange = new HttpExchange(
                httpRequest,
                httpResponse,
                210,
                OffsetDateTime.now(ZoneId.of("UTC")),
                new AtomicInteger(1),
                false);
        queue.offer(QueueProducer.toKafkaRecord(httpExchange));
        List<SourceRecord> sourceRecords = wsSourceTask.poll();
        long successfulMessagesCount = sourceRecords.stream().filter(sourceRecord -> sourceRecord.topic().equals(getNominalConfig().get(SUCCESS_TOPIC))).count();
        assertThat(successfulMessagesCount).isEqualTo(0);
        long errorMessagesCount = sourceRecords.stream().filter(sourceRecord -> sourceRecord.topic().equals(getNominalConfig().get(ERROR_TOPIC))).count();
        assertThat(errorMessagesCount).isEqualTo(1);
        //we have consumed all messages
        assertThat(queue).isEmpty();
    }

}