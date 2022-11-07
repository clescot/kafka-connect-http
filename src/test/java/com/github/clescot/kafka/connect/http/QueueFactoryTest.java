package com.github.clescot.kafka.connect.http;

import com.github.clescot.kafka.connect.http.source.Acknowledgement;
import org.junit.jupiter.api.Test;

import java.util.Queue;

import static com.github.clescot.kafka.connect.http.QueueFactory.DEFAULT_QUEUE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class QueueFactoryTest {


    @Test
    public void test_get_queue_without_queue_name(){
        Queue<Acknowledgement> queue = QueueFactory.getQueue();
        assertThat(queue).isNotNull();
        Queue<Acknowledgement> queue2 = QueueFactory.getQueue();
        assertThat(queue2 == queue);
    }

    @Test
    public void test_get_queue_with_queue_name(){
        Queue<Acknowledgement> queue = QueueFactory.getQueue(DEFAULT_QUEUE_NAME);
        assertThat(queue).isNotNull();
        Queue<Acknowledgement> queue2 = QueueFactory.getQueue();
        assertThat(queue2 == queue);
        Queue<Acknowledgement> queue3 = QueueFactory.getQueue("dummy");
        assertThat(queue3 != queue);
        Queue<Acknowledgement> queue4 = QueueFactory.getQueue("dummy");
        assertThat(queue3 == queue4);
        Queue<Acknowledgement> queue5 = QueueFactory.getQueue("dummy2");
        assertThat(queue5 != queue4);
        assertThat(queue5 != queue);
    }


}