package io.github.clescot.queue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Queue;

import static io.github.clescot.client.queue.QueueFactory.DEFAULT_QUEUE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

class QueueFactoryTest {


    @Test
    void test_get_queue_without_queue_name(){
        Queue<?> queue = io.github.clescot.client.queue.QueueFactory.getQueue();
        assertThat(queue).isNotNull();
        Queue<?> queue2 = io.github.clescot.client.queue.QueueFactory.getQueue();
        assertThat(queue2).isSameAs(queue);
    }

    @Test
    void test_get_queue_with_queue_name(){
        Queue<?> queue = io.github.clescot.client.queue.QueueFactory.getQueue(DEFAULT_QUEUE_NAME);
        assertThat(queue).isNotNull();
        Queue<?> queue2 = io.github.clescot.client.queue.QueueFactory.getQueue();
        assertThat(queue2).isSameAs(queue);
        Queue<?> queue3 = io.github.clescot.client.queue.QueueFactory.getQueue("dummy");
        assertThat(queue3).isNotSameAs(queue);
        Queue<?> queue4 = io.github.clescot.client.queue.QueueFactory.getQueue("dummy");
        assertThat(queue3).isSameAs(queue4);
        Queue<?> queue5 = io.github.clescot.client.queue.QueueFactory.getQueue("dummy2");
        assertThat(queue5)
                .isNotSameAs(queue4)
                .isNotSameAs(queue);
    }


    @Test
    void test_registerConsumerForQueue(){
        io.github.clescot.client.queue.QueueFactory.registerConsumerForQueue("test");
        assertThat(io.github.clescot.client.queue.QueueFactory.hasAConsumer("test",200, 2000, 500)).isTrue();
    }

    @Test
    void test_registerConsumerForQueue_with_null_value(){
        Assertions.assertThrows(NullPointerException.class,()->
                io.github.clescot.client.queue.QueueFactory.registerConsumerForQueue(null)
                );
    }

    @Test
    void test_registerConsumerForQueue_with_an_empty_value(){
        Assertions.assertThrows(IllegalArgumentException.class,()->
                io.github.clescot.client.queue.QueueFactory.registerConsumerForQueue("")
                );
    }


    @Test
    void test_clear_registrations(){
        //given
        String queueName = "test";
        io.github.clescot.client.queue.QueueFactory.registerConsumerForQueue(queueName);
        assertThat(io.github.clescot.client.queue.QueueFactory.hasAConsumer(queueName,500, 2000, 500)).isTrue();

        //when
        io.github.clescot.client.queue.QueueFactory.clearRegistrations();
        assertThat(io.github.clescot.client.queue.QueueFactory.hasAConsumer(queueName,500, 2000, 500)).isFalse();
    }


    @Test
    void test_has_not_a_queue_name_with_timeout(){
        String queueName = "test";
        //given
        io.github.clescot.client.queue.QueueFactory.getQueue(queueName);
        //when
        boolean hasAConsumer = io.github.clescot.client.queue.QueueFactory.hasAConsumer(queueName, 500, 2000, 500);
        //then
        assertThat(hasAConsumer).isFalse();
    }

    @Test
    void test_has_a_queue_name_with_timeout(){
        String queueName = "test";
        //given
        io.github.clescot.client.queue.QueueFactory.getQueue(queueName);
        io.github.clescot.client.queue.QueueFactory.registerConsumerForQueue(queueName);
        //when
        boolean hasAConsumer = io.github.clescot.client.queue.QueueFactory.hasAConsumer(queueName, 500, 2000, 500);
        //then
        assertThat(hasAConsumer).isTrue();
    }

    @AfterEach
    void tearsDown(){
        io.github.clescot.client.queue.QueueFactory.clearRegistrations();
    }
}