package io.github.clescot.kafka.connect.http.sink;

import org.apache.kafka.clients.producer.MockProducer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


class AHCSinkTaskTest {

    @Test
    void test_constructor_with_no_args(){
        Assertions.assertDoesNotThrow(()-> new AHCSinkTask());
    }

    @Test
    void test_constructor_with_mock_producer(){
        try (MockProducer<String, Object> mockProducer = new MockProducer<>()) {
            Assertions.assertDoesNotThrow(() -> new AHCSinkTask(mockProducer));
        }
    }
}