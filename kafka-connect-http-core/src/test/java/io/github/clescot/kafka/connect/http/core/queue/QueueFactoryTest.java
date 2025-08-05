package io.github.clescot.kafka.connect.http.core.queue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Queue;

import static io.github.clescot.kafka.connect.http.core.queue.QueueFactory.DEFAULT_QUEUE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

class QueueFactoryTest {


    @Test
    void test_get_queue_without_queue_name(){
        Queue<KafkaRecord> queue = QueueFactory.getQueue();
        assertThat(queue).isNotNull();
        Queue<KafkaRecord> queue2 = QueueFactory.getQueue();
        assertThat(queue2).isSameAs(queue);
    }

    @Test
    void test_get_queue_with_queue_name(){
        Queue<KafkaRecord> queue = QueueFactory.getQueue(DEFAULT_QUEUE_NAME);
        assertThat(queue).isNotNull();
        Queue<KafkaRecord> queue2 = QueueFactory.getQueue();
        assertThat(queue2).isSameAs(queue);
        Queue<KafkaRecord> queue3 = QueueFactory.getQueue("dummy");
        assertThat(queue3).isNotSameAs(queue);
        Queue<KafkaRecord> queue4 = QueueFactory.getQueue("dummy");
        assertThat(queue3).isSameAs(queue4);
        Queue<KafkaRecord> queue5 = QueueFactory.getQueue("dummy2");
        assertThat(queue5)
                .isNotSameAs(queue4)
                .isNotSameAs(queue);
    }


    @Test
    void test_registerConsumerForQueue(){
        QueueFactory.registerConsumerForQueue("test");
        assertThat(QueueFactory.hasAConsumer("test",200, 2000, 500)).isTrue();
    }

    @Test
    void test_registerConsumerForQueue_with_null_value(){
        Assertions.assertThrows(NullPointerException.class,()->
                QueueFactory.registerConsumerForQueue(null)
                );
    }

    @Test
    void test_registerConsumerForQueue_with_an_empty_value(){
        Assertions.assertThrows(IllegalArgumentException.class,()->
                QueueFactory.registerConsumerForQueue("")
                );
    }


    @Test
    void test_clear_registrations(){
        //given
        String queueName = "test";
        QueueFactory.registerConsumerForQueue(queueName);
        assertThat(QueueFactory.hasAConsumer(queueName,500, 2000, 500)).isTrue();

        //when
        QueueFactory.clearRegistrations();
        assertThat(QueueFactory.hasAConsumer(queueName,500, 2000, 500)).isFalse();
    }


    @Test
    void test_has_not_a_queue_name_with_timeout(){
        String queueName = "test";
        //given
        QueueFactory.getQueue(queueName);
        //when
        boolean hasAConsumer = QueueFactory.hasAConsumer(queueName, 500, 2000, 500);
        //then
        assertThat(hasAConsumer).isFalse();
    }

    @Test
    void test_has_a_queue_name_with_timeout(){
        String queueName = "test";
        //given
        QueueFactory.getQueue(queueName);
        QueueFactory.registerConsumerForQueue(queueName);
        //when
        boolean hasAConsumer = QueueFactory.hasAConsumer(queueName, 500, 2000, 500);
        //then
        assertThat(hasAConsumer).isTrue();
    }

    @AfterEach
    void tearsDown(){
        QueueFactory.clearRegistrations();
    }
}