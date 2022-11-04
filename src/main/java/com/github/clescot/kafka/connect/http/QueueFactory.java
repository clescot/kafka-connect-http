package com.github.clescot.kafka.connect.http;

import com.github.clescot.kafka.connect.http.source.Acknowledgement;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class QueueFactory {
    public static final String DEFAULT_QUEUE_NAME = "default";
    private static Map<String,Queue<Acknowledgement>> queueMap = Maps.newHashMap();
    //TODO ad a map of queue
    public static synchronized Queue<Acknowledgement> getQueue(String queueName){
        if(queueMap.get(queueName) == null){
            queueMap.put(queueName, new ConcurrentLinkedQueue<>());
        }
        return queueMap.get(queueName);
    }
    public static synchronized Queue<Acknowledgement> getQueue(){
        return getQueue(DEFAULT_QUEUE_NAME);
    }
}
