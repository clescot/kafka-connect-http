package com.github.clescot.kafka.connect.http.sink;

import com.github.clescot.kafka.connect.http.sink.config.ConfigConstants;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

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

}