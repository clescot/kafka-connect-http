package com.github.clescot.kafka.connect.http.sink;

import com.github.clescot.kafka.connect.http.sink.config.ConfigConstants;
import com.github.clescot.kafka.connect.http.sink.service.WsCaller;
import com.github.clescot.kafka.connect.http.source.Acknowledgement;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class WsSinkTaskTest {

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
        WsCaller wsCaller = mock(WsCaller.class);
        when(wsCaller.call(any(SinkRecord.class))).thenReturn(getDummyAcknowledgment());
        wsSinkTask.setWsCaller(wsCaller);
        List<SinkRecord> records = Lists.newArrayList();
        List<Header> headers = Lists.newArrayList();
        SinkRecord sinkRecord = new SinkRecord("myTopic",0, Schema.STRING_SCHEMA,"key",Schema.STRING_SCHEMA,"myValue",-1,System.currentTimeMillis(), TimestampType.CREATE_TIME,headers);
        records.add(sinkRecord);
        wsSinkTask.put(records);
        ArgumentCaptor<SinkRecord> captor = ArgumentCaptor.forClass(SinkRecord.class);
        verify(wsCaller,times(1)).call(captor.capture());
        SinkRecord enhancedRecord = captor.getValue();
        assertThat(enhancedRecord.headers()).anyMatch(header -> "param1".equals(header.key())&& "value1".equals(header.value()));
        assertThat(enhancedRecord.headers()).anyMatch(header -> "param2".equals(header.key())&& "value2".equals(header.value()));
    }

    private Acknowledgement getDummyAcknowledgment() {
        return new Acknowledgement(
                "fsdfsf--sdfsdfsdf",
                "fsdfsdf5565",
                200,
                "OK",
                Maps.newHashMap(),
                "",
                "http://www.dummy.com",
                Maps.newHashMap(),
                "GET",
                "",
                100L,
                OffsetDateTime.now(ZoneId.of("UTC")),
                new AtomicInteger(1),
                true
        );
    }

}