package com.github.clescot.kafka.connect.http;

import com.github.clescot.kafka.connect.http.source.Acknowledgement;

import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

public class QueueFactory {
    private static TransferQueue<Acknowledgement> queue;

    public static synchronized TransferQueue<Acknowledgement> getQueue(){
        if(queue == null){
            queue = new LinkedTransferQueue<>();
        }
        return queue;
    }
}
