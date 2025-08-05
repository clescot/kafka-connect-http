package io.github.clescot.kafka.connect.http.sink;


import io.github.clescot.kafka.connect.http.MessageSplitter;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlFeatures;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static io.github.clescot.kafka.connect.http.sink.HttpSinkTask.FROM_STRING_PART_TO_SINK_RECORD_FUNCTION;

class MessageSplitterTest {
    @Nested
    class Constructor {

        @Test
        void test_with_null_params(){
            Assertions.assertThrows(NullPointerException.class,()->new MessageSplitter(null,null,null,null,0,FROM_STRING_PART_TO_SINK_RECORD_FUNCTION));
        }

        @Test
        void test_with_null_id(){
            JexlEngine jexlEngine = buildJexlEngine();
            Assertions.assertThrows(NullPointerException.class,()->new MessageSplitter(null,jexlEngine,"","\\n",0,FROM_STRING_PART_TO_SINK_RECORD_FUNCTION));
        }

        @Test
        void test_with_null_matching_expression(){
            JexlEngine jexlEngine = buildJexlEngine();
            Assertions.assertThrows(NullPointerException.class,()->new MessageSplitter("config1",jexlEngine,null,"\\n",0,FROM_STRING_PART_TO_SINK_RECORD_FUNCTION));
        }

        @Test
        void test_with_null_jexl_engine(){
            Assertions.assertThrows(NullPointerException.class,()->new MessageSplitter("myid",null,"message.splitter.myid.matcher","\\n",0,FROM_STRING_PART_TO_SINK_RECORD_FUNCTION));
        }

        @Test
        void test_with_null_split_pattern(){
            JexlEngine jexlEngine = buildJexlEngine();
            Assertions.assertThrows(NullPointerException.class,()->new MessageSplitter("myid",jexlEngine,"message.splitter.myid.matcher",null,0,FROM_STRING_PART_TO_SINK_RECORD_FUNCTION));
        }

        @Test
        void test_with_no_split_limit(){
            JexlEngine jexlEngine = buildJexlEngine();
            Assertions.assertDoesNotThrow(()->new MessageSplitter("myid",jexlEngine,"message.splitter.myid.matcher","\\n",0,FROM_STRING_PART_TO_SINK_RECORD_FUNCTION));
        }


    }

    private static JexlEngine buildJexlEngine() {
        // Restricted permissions to a safe set but with URI allowed
        JexlPermissions permissions = new JexlPermissions.ClassPermissions(SinkRecord.class, ConnectRecord.class, HttpRequest.class);
        // Create the engine
        JexlFeatures features = new JexlFeatures()
                .loops(false)
                .sideEffectGlobal(false)
                .sideEffect(false);
        return new JexlBuilder().features(features).permissions(permissions).create();
    }
}