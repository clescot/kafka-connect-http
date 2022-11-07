package com.github.clescot.kafka.connect.http;

import com.github.clescot.kafka.connect.http.source.Acknowledgement;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class QueueFactory {
    private static Queue<Acknowledgement> queue;

    public static synchronized Queue<Acknowledgement> getQueue(){
        if(queue == null){
            queue = new ConcurrentLinkedQueue<>();
        }
        return queue;
    }
}
