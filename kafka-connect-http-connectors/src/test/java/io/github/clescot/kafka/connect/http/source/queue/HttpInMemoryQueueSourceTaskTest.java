package io.github.clescot.kafka.connect.http.source.queue;

import com.google.common.collect.Maps;
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

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.clescot.kafka.connect.http.source.queue.HttpInMemoryQueueSourceConfigDefinition.ERROR_TOPIC;
import static io.github.clescot.kafka.connect.http.source.queue.HttpInMemoryQueueSourceConfigDefinition.SUCCESS_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

class HttpInMemoryQueueSourceTaskTest {
    private HttpInMemoryQueueSourceTask wsSourceTask;

    @BeforeEach
    void setup() {
        wsSourceTask = new HttpInMemoryQueueSourceTask();
    }

    @AfterEach
    void tearsDown() {
        Queue<KafkaRecord> queue = QueueFactory.getQueue();
        queue.clear();
    }

    @Test
    void test_start_with_null_settings() {
        wsSourceTask = new HttpInMemoryQueueSourceTask();

        Assertions.assertThrows(NullPointerException.class, () -> wsSourceTask.start(null));
    }

    @Test
    void test_start_with_empty_settings() {
        HashMap<String, String> taskConfig = Maps.newHashMap();
        Assertions.assertThrows(ConfigException.class, () -> wsSourceTask.start(taskConfig));
    }

    @Test
    void test_start_nominal_case() {
        Map<String, String> config = getNominalConfig();
        Assertions.assertDoesNotThrow( () -> wsSourceTask.start(config));
    }

    @NotNull
    private static Map<String, String> getNominalConfig() {
        Map<String, String> config = Maps.newHashMap();
        config.put(SUCCESS_TOPIC, "http-success");
        config.put(ERROR_TOPIC, "http-error");
        return config;
    }

    @Test
    void poll() throws ExecutionException, InterruptedException {
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
    void test_success() {
        wsSourceTask.start(getNominalConfig());
        Queue<KafkaRecord> queue = QueueFactory.getQueue();
        HttpRequest httpRequest = new HttpRequest(
                "http://www.dummy.com",
                HttpRequest.Method.GET
        );
        httpRequest.setBodyAsString("stuff");
        HttpResponse httpResponse = new HttpResponse(200, "OK");
        httpResponse.setBodyAsString("dummy response");
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
        assertThat(errorMessagesCount).isZero();
        //we have consumed all messages
        assertThat(queue).isEmpty();
    }

    @Test
    void test_error() {
        wsSourceTask.start(getNominalConfig());
        Queue<KafkaRecord> queue = QueueFactory.getQueue();
        HttpRequest httpRequest = new HttpRequest(
                "http://www.dummy.com",
                HttpRequest.Method.GET
        );
        httpRequest.setBodyAsString("stuff");
        HttpResponse httpResponse = new HttpResponse(500, "Internal Server Error");
        httpResponse.setBodyAsString("dummy response");
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
        assertThat(successfulMessagesCount).isZero();
        long errorMessagesCount = sourceRecords.stream().filter(sourceRecord -> sourceRecord.topic().equals(getNominalConfig().get(ERROR_TOPIC))).count();
        assertThat(errorMessagesCount).isEqualTo(1);
        //we have consumed all messages
        assertThat(queue).isEmpty();
    }

}