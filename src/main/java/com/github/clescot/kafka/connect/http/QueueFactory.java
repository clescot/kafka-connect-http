package com.github.clescot.kafka.connect.http;

import com.github.clescot.kafka.connect.http.source.Acknowledgement;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class QueueFactory {
    public static final String DEFAULT_QUEUE_NAME = "default";
    private static Logger LOGGER = LoggerFactory.getLogger(QueueFactory.class);
    private static Map<String,Queue<Acknowledgement>> queueMap = Maps.newHashMap();
    //TODO ad a map of queue
    public static synchronized Queue<Acknowledgement> getQueue(String queueName){
        if(queueMap.get(queueName) == null){
            LOGGER.debug("creating the '{}' queue",queueName);
            queueMap.put(queueName, new ConcurrentLinkedQueue<>());
        }
        return queueMap.get(queueName);
    }
    public static synchronized Queue<Acknowledgement> getQueue(){
        return getQueue(DEFAULT_QUEUE_NAME);
    }

    public static boolean queueMapIsEmpty(){
        return queueMap.isEmpty();
    }
}
