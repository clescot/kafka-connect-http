package com.github.clescot.kafka.connect.http.sink;

import com.github.clescot.kafka.connect.http.QueueFactory;
import com.github.clescot.kafka.connect.http.sink.client.HttpClient;
import com.github.clescot.kafka.connect.http.source.Acknowledgement;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.clescot.kafka.connect.http.sink.ConfigConstants.PUBLISH_TO_IN_MEMORY_QUEUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class WsSinkTaskTest {



    @BeforeEach
    public void setUp(){
        QueueFactory.clearRegistrations();
    }

    @Test
    public void test_start_with_queue_name(){
        WsSinkTask wsSinkTask = new WsSinkTask();
        Map<String,String> settings = Maps.newHashMap();
        settings.put(ConfigConstants.QUEUE_NAME,"dummyQueueName");
        wsSinkTask.start(settings);
    }

    @Test
    public void test_start_with_static_request_headers(){
        WsSinkTask wsSinkTask = new WsSinkTask();
        Map<String,String> settings = Maps.newHashMap();
        settings.put(ConfigConstants.STATIC_REQUEST_HEADER_NAMES,"param1,param2");
        settings.put("param1","value1");
        settings.put("param2","value2");
        wsSinkTask.start(settings);
    }

    @Test
    public void test_start_with_static_request_headers_without_required_parameters(){
        Assertions.assertThrows(NullPointerException.class,()->{
            WsSinkTask wsSinkTask = new WsSinkTask();
            Map<String,String> settings = Maps.newHashMap();
            settings.put(ConfigConstants.STATIC_REQUEST_HEADER_NAMES,"param1,param2");
            wsSinkTask.start(settings);
        });

    }


    @Test
    public void test_start_no_settings(){
        WsSinkTask wsSinkTask = new WsSinkTask();
        wsSinkTask.start(Maps.newHashMap());
    }


    @Test
    public void test_put_add_static_headers(){
        WsSinkTask wsSinkTask = new WsSinkTask();
        Map<String,String> settings = Maps.newHashMap();
        settings.put(ConfigConstants.STATIC_REQUEST_HEADER_NAMES,"param1,param2");
        settings.put("param1","value1");
        settings.put("param2","value2");
        wsSinkTask.start(settings);
        HttpClient httpClient = mock(HttpClient.class);
        Acknowledgement dummyAcknowledgment = getDummyAcknowledgment();
        when(httpClient.call(any(SinkRecord.class))).thenReturn(dummyAcknowledgment);
        wsSinkTask.setHttpClient(httpClient);
        List<SinkRecord> records = Lists.newArrayList();
        List<Header> headers = Lists.newArrayList();
        SinkRecord sinkRecord = new SinkRecord("myTopic",0, Schema.STRING_SCHEMA,"key",Schema.STRING_SCHEMA,"myValue",-1,System.currentTimeMillis(), TimestampType.CREATE_TIME,headers);
        records.add(sinkRecord);
        wsSinkTask.put(records);
        ArgumentCaptor<SinkRecord> captor = ArgumentCaptor.forClass(SinkRecord.class);
        verify(httpClient,times(1)).call(captor.capture());
        SinkRecord enhancedRecordBeforeHttpCall = captor.getValue();
        assertThat(enhancedRecordBeforeHttpCall.headers().size()==sinkRecord.headers().size()+wsSinkTask.getStaticRequestHeaders().size());
        assertThat(enhancedRecordBeforeHttpCall.headers()).anyMatch(header -> "param1".equals(header.key())&& "value1".equals(header.value()));
        assertThat(enhancedRecordBeforeHttpCall.headers()).anyMatch(header -> "param2".equals(header.key())&& "value2".equals(header.value()));
    }

    @Test
    public void test_put_nominal_case(){
        //given
        WsSinkTask wsSinkTask = new WsSinkTask();
        Map<String,String> settings = Maps.newHashMap();
        wsSinkTask.start(settings);

        //mock httpClient
        HttpClient httpClient = mock(HttpClient.class);
        Acknowledgement dummyAcknowledgment = getDummyAcknowledgment();
        when(httpClient.call(any(SinkRecord.class))).thenReturn(dummyAcknowledgment);
        wsSinkTask.setHttpClient(httpClient);

        //mock queue
        Queue<Acknowledgement> dummyQueue = mock(Queue.class);
        wsSinkTask.setQueue(dummyQueue);

        //init sinkRecord
        List<SinkRecord> records = Lists.newArrayList();
        List<Header> headers = Lists.newArrayList();
        SinkRecord sinkRecord = new SinkRecord("myTopic",0, Schema.STRING_SCHEMA,"key",Schema.STRING_SCHEMA,"myValue",-1,System.currentTimeMillis(), TimestampType.CREATE_TIME,headers);
        records.add(sinkRecord);

        //when
        wsSinkTask.put(records);

        //then

        //no additional headers added
        ArgumentCaptor<SinkRecord> captor = ArgumentCaptor.forClass(SinkRecord.class);
        verify(httpClient,times(1)).call(captor.capture());
        SinkRecord enhancedRecordBeforeHttpCall = captor.getValue();
        assertThat(enhancedRecordBeforeHttpCall.headers().size()==sinkRecord.headers().size());

        //no records are published into the in memory queue by default
        verify(dummyQueue,never()).offer(any(Acknowledgement.class));
    }

    @Test
    public void test_put_with_publish_to_in_memory_queue_without_consumer(){
        //given
        WsSinkTask wsSinkTask = new WsSinkTask();
        Map<String,String> settings = Maps.newHashMap();
        settings.put(PUBLISH_TO_IN_MEMORY_QUEUE,"true");
        wsSinkTask.start(settings);

        //mock httpClient
        HttpClient httpClient = mock(HttpClient.class);
        Acknowledgement dummyAcknowledgment = getDummyAcknowledgment();
        when(httpClient.call(any(SinkRecord.class))).thenReturn(dummyAcknowledgment);
        wsSinkTask.setHttpClient(httpClient);

        //mock queue
        Queue<Acknowledgement> dummyQueue = mock(Queue.class);
        wsSinkTask.setQueue(dummyQueue);

        //init sinkRecord
        List<SinkRecord> records = Lists.newArrayList();
        List<Header> headers = Lists.newArrayList();
        SinkRecord sinkRecord = new SinkRecord("myTopic",0, Schema.STRING_SCHEMA,"key",Schema.STRING_SCHEMA,"myValue",-1,System.currentTimeMillis(), TimestampType.CREATE_TIME,headers);
        records.add(sinkRecord);

        //when
        //then
        Assertions.assertThrows(IllegalArgumentException.class,
                ()->wsSinkTask.put(records));

    }




    @Test
    public void test_put_with_publish_in_memory_set_to_false(){
        WsSinkTask wsSinkTask = new WsSinkTask();
        Map<String,String> settings = Maps.newHashMap();
        settings.put(PUBLISH_TO_IN_MEMORY_QUEUE,"false");
        wsSinkTask.start(settings);
        HttpClient httpClient = mock(HttpClient.class);
        Acknowledgement dummyAcknowledgment = getDummyAcknowledgment();
        when(httpClient.call(any(SinkRecord.class))).thenReturn(dummyAcknowledgment);
        wsSinkTask.setHttpClient(httpClient);
        Queue<Acknowledgement> queue = mock(Queue.class);
        wsSinkTask.setQueue(queue);
        List<SinkRecord> records = Lists.newArrayList();
        List<Header> headers = Lists.newArrayList();
        SinkRecord sinkRecord = new SinkRecord("myTopic",0, Schema.STRING_SCHEMA,"key",Schema.STRING_SCHEMA,"myValue",-1,System.currentTimeMillis(), TimestampType.CREATE_TIME,headers);
        records.add(sinkRecord);
        wsSinkTask.put(records);
        verify(httpClient,times(1)).call(any(SinkRecord.class));
        verify(queue,never()).offer(any(Acknowledgement.class));
    }

    @Test
    public void test_put_with_publish_to_in_memory_queue_set_to_true_with_a_consumer(){

        //given
        WsSinkTask wsSinkTask = new WsSinkTask();
        Map<String,String> settings = Maps.newHashMap();
        settings.put(PUBLISH_TO_IN_MEMORY_QUEUE,"true");
        QueueFactory.registerConsumerForQueue(QueueFactory.DEFAULT_QUEUE_NAME);
        wsSinkTask.start(settings);
        HttpClient httpClient = mock(HttpClient.class);
        Acknowledgement dummyAcknowledgment = getDummyAcknowledgment();
        when(httpClient.call(any(SinkRecord.class))).thenReturn(dummyAcknowledgment);
        wsSinkTask.setHttpClient(httpClient);
        Queue<Acknowledgement> queue = mock(Queue.class);
        wsSinkTask.setQueue(queue);
        List<SinkRecord> records = Lists.newArrayList();
        List<Header> headers = Lists.newArrayList();
        SinkRecord sinkRecord = new SinkRecord("myTopic",0, Schema.STRING_SCHEMA,"key",Schema.STRING_SCHEMA,"myValue",-1,System.currentTimeMillis(), TimestampType.CREATE_TIME,headers);
        records.add(sinkRecord);
        //when
        wsSinkTask.put(records);

        //then
        verify(httpClient,times(1)).call(any(SinkRecord.class));
        verify(queue,times(1)).offer(any(Acknowledgement.class));
    }


    private Acknowledgement getDummyAcknowledgment() {
        HashMap<String, String> requestHeaders = Maps.newHashMap();
        requestHeaders.put("X-dummy","blabla");
        HashMap<String, String> responseHeaders = Maps.newHashMap();
        responseHeaders.put("Content-Type","application/json");
        return new Acknowledgement(
                "fsdfsf--sdfsdfsdf",
                "fsdfsdf5565",
                200,
                "OK",
                responseHeaders,
                "",
                "http://www.dummy.com",
                requestHeaders,
                "GET",
                "",
                100L,
                OffsetDateTime.now(ZoneId.of("UTC")),
                new AtomicInteger(1),
                true
        );
    }

}