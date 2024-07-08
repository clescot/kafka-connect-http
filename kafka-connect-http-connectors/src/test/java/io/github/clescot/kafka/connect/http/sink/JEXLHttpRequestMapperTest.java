package io.github.clescot.kafka.connect.http.sink;

import com.google.common.collect.Lists;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.sink.mapper.JEXLHttpRequestMapper;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlFeatures;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class JEXLHttpRequestMapperTest {

    private static final String DUMMY_BODY = "stuff";
    private static final String DUMMY_URL = "http://www." + DUMMY_BODY + ".com";
    private static final String DUMMY_METHOD = "POST";
    private static final String DUMMY_BODY_TYPE = "STRING";

    private JexlEngine jexlEngine;
    @BeforeEach
    void setUp() {
        // Restricted permissions to a safe set but with URI allowed
        JexlPermissions permissions = new JexlPermissions.ClassPermissions(SinkRecord.class, ConnectRecord.class,HttpRequest.class);
        // Create the engine
        JexlFeatures features = new JexlFeatures()
                .loops(false)
                .sideEffectGlobal(false)
                .sideEffect(false);
        jexlEngine = new JexlBuilder().features(features).permissions(permissions).create();
    }

    @Nested
    class Constructor {


        @Test
        void test_empty_expression() {
            Assertions.assertThrows(IllegalArgumentException.class,()->new JEXLHttpRequestMapper(jexlEngine,"","",null,null,null,null));
        }
        @Test
        void test_null_expression() {
            Assertions.assertThrows(NullPointerException.class,()->new JEXLHttpRequestMapper(jexlEngine,null,"'http://url.com'",null,null,null,null));
        }

        @Test
        void test_valid_expression_with_url_as_constant() {
            Assertions.assertDoesNotThrow(()->new JEXLHttpRequestMapper(jexlEngine,"sinkRecord.topic()=='myTopic'","'http://url.com'",null,null,null,null));
        }

        @Test
        void test_valid_expression_with_url_from_sink_record() {
            Assertions.assertDoesNotThrow(()->new JEXLHttpRequestMapper(jexlEngine, "sinkRecord.topic()=='myTopic'", "sinkRecord.value()", null, null, null, null));

        }
        @Test
        void test_valid_expression_with_url_and_method_from_sink_record() {
            Assertions.assertDoesNotThrow(()->new JEXLHttpRequestMapper(jexlEngine, "sinkRecord.topic()=='myTopic'", "sinkRecord.value()", "'POST'", null, null, null));
        }
        @Test
        void test_valid_expression_with_url_method_and_body_from_sink_record() {
            Assertions.assertDoesNotThrow(()->new JEXLHttpRequestMapper(jexlEngine, "sinkRecord.topic()=='myTopic'", "sinkRecord.value().split(\"#\")[0]", "'POST'", null, "sinkRecord.value().split(\"#\")[1]", null));
        }

    }


    @Nested
    class Matches {


        private SinkRecord sinkRecord;
        @BeforeEach
        public void setup() {
            List<Header> headers = Lists.newArrayList();
            sinkRecord = new SinkRecord("myTopic",
                    0,
                    Schema.STRING_SCHEMA,
                    "key",
                    Schema.STRING_SCHEMA,
                    getDummyHttpRequestAsString(),
                    -1,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME,
                    headers
            );

        }

        @Test
        void test_nominal() {
            JEXLHttpRequestMapper httpRequestMapper = new JEXLHttpRequestMapper(jexlEngine,"sinkRecord.topic()=='myTopic'","'http://url.com'",null,null,null,null);
            boolean matches = httpRequestMapper.matches(sinkRecord);
            assertThat(matches).isTrue();
        }

        @Test
        void test_invalid_expression() {
            JEXLHttpRequestMapper httpRequestMapper = new JEXLHttpRequestMapper(jexlEngine,"toto.topic()=='myTopic'","'http://url.com'",null,null,null,null);
            boolean matches = httpRequestMapper.matches(sinkRecord);
            assertThat(matches).isFalse();
        }


    }

    @Nested
    class Map {

        private static final String DUMMY_BODY = "stuff";
        private static final String DUMMY_URL = "http://www." + DUMMY_BODY + ".com";
        private static final String DUMMY_METHOD = "POST";
        private static final String DUMMY_BODY_TYPE = "STRING";
        private SinkRecord sinkRecord;
        @BeforeEach
        public void setup() {
            List<Header> headers = Lists.newArrayList();
            sinkRecord = new SinkRecord("myTopic",
                    0,
                    Schema.STRING_SCHEMA,
                    "key",
                    Schema.STRING_SCHEMA,
                    "http://test.com",
                    -1,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME,
                    headers
            );

        }
        @Test
        void test_url_as_constant(){
            JEXLHttpRequestMapper httpRequestMapper = new JEXLHttpRequestMapper(jexlEngine,"sinkRecord.topic()=='myTopic'","'http://url.com'",null,null,null,null);
            boolean matches = httpRequestMapper.matches(sinkRecord);
            assertThat(matches).isTrue();
            HttpRequest httpRequest = httpRequestMapper.map(sinkRecord);
            assertThat(httpRequest.getUrl()).isEqualTo("http://url.com");
            assertThat(httpRequest.getMethod()).isEqualTo(HttpRequest.Method.GET);
            assertThat(httpRequest.getBodyType()).isEqualTo(HttpRequest.BodyType.STRING);
        }
        @Test
        void test_url_as_variable(){
            JEXLHttpRequestMapper httpRequestMapper = new JEXLHttpRequestMapper(jexlEngine,"sinkRecord.topic()=='myTopic'","sinkRecord.value()",null,null,null,null);
            boolean matches = httpRequestMapper.matches(sinkRecord);
            assertThat(matches).isTrue();
            HttpRequest httpRequest = httpRequestMapper.map(sinkRecord);
            assertThat(httpRequest.getUrl()).isEqualTo("http://test.com");
            assertThat(httpRequest.getMethod().name()).isEqualTo("GET");
            assertThat(httpRequest.getBodyType()).isEqualTo(HttpRequest.BodyType.STRING);
        }
        @Test
        void test_with_all_variables(){
            JEXLHttpRequestMapper httpRequestMapper = new JEXLHttpRequestMapper(
                    jexlEngine,
                    "sinkRecord.topic()=='myTopic'",
                    "sinkRecord.value()",
                    "'GET'",
                    "'"+ HttpRequest.BodyType.STRING +"'",
                    "'content'",
                    "{'test1':['value1','value2',...]}"
            );
            boolean matches = httpRequestMapper.matches(sinkRecord);
            assertThat(matches).isTrue();
            HttpRequest httpRequest = httpRequestMapper.map(sinkRecord);
            assertThat(httpRequest.getUrl()).isEqualTo("http://test.com");
            assertThat(httpRequest.getMethod()).isEqualTo(HttpRequest.Method.GET);
            assertThat(httpRequest.getBodyType()).isEqualTo(HttpRequest.BodyType.STRING);
            java.util.Map<String, List<String>> httpRequestHeaders = httpRequest.getHeaders();
            assertThat(httpRequestHeaders.get("test1")).isEqualTo(Lists.newArrayList("value1","value2"));
        }


        @Test
        void test_valid_expression_with_url_from_sink_record() {
            JEXLHttpRequestMapper jexlHttpRequestMapper = new JEXLHttpRequestMapper(jexlEngine, "sinkRecord.topic()=='myTopic'", "sinkRecord.value()", null, null, null, null);
            String url = "http://test.com";
            SinkRecord sinkRecord = new SinkRecord("test",0,Schema.STRING_SCHEMA,"123",Schema.STRING_SCHEMA, url,0);
            HttpRequest httpRequest = jexlHttpRequestMapper.map(sinkRecord);
            assertThat(httpRequest.getUrl()).isEqualTo(url);
        }
        @Test
        void test_valid_expression_with_url_and_method_from_sink_record() {
            JEXLHttpRequestMapper jexlHttpRequestMapper = new JEXLHttpRequestMapper(jexlEngine, "sinkRecord.topic()=='myTopic'", "sinkRecord.value()", "'POST'", null, null, null);
            String url = "http://test.com";
            SinkRecord mySinkRecord = new SinkRecord("test",0,Schema.STRING_SCHEMA,"123",Schema.STRING_SCHEMA, url,0);
            HttpRequest httpRequest = jexlHttpRequestMapper.map(mySinkRecord);
            assertThat(httpRequest.getUrl()).isEqualTo(url);
            assertThat(httpRequest.getMethod()).isEqualTo(HttpRequest.Method.POST);
        }
        @Test
        void test_valid_expression_with_url_method_and_body_from_sink_record() {
            JEXLHttpRequestMapper jexlHttpRequestMapper = new JEXLHttpRequestMapper(jexlEngine, "sinkRecord.topic()=='myTopic'", "sinkRecord.value().split(\"#\")[0]", "'POST'", null, "sinkRecord.value().split(\"#\")[1]", null);
            String value = "http://test.com#mybodycontent";
            SinkRecord mySinkRecord = new SinkRecord("test",0,Schema.STRING_SCHEMA,"123",Schema.STRING_SCHEMA, value,0);
            HttpRequest httpRequest = jexlHttpRequestMapper.map(mySinkRecord);
            assertThat(httpRequest.getUrl()).isEqualTo("http://test.com");
            assertThat(httpRequest.getMethod()).isEqualTo(HttpRequest.Method.POST);
            assertThat(httpRequest.getBodyAsString()).isEqualTo("mybodycontent");
        }

        @Test
        void test_valid_expression_with_all_resolved_variables_from_sink_record() {
            String value = "http://test.com#PUT#STRING#mybodycontent#key1-value1/key2-value2";
            JEXLHttpRequestMapper jexlHttpRequestMapper = new JEXLHttpRequestMapper(
                    jexlEngine,
                    "sinkRecord.topic()=='myTopic'",
                    "sinkRecord.value().split(\"#\")[0]",
                    "sinkRecord.value().split(\"#\")[1]",
                    "sinkRecord.value().split(\"#\")[2]",
                    "sinkRecord.value().split(\"#\")[3]",
                    "{'test1':['value1','value2',...]}"
            );
            SinkRecord mySinkRecord = new SinkRecord("test",0,Schema.STRING_SCHEMA,"123",Schema.STRING_SCHEMA, value,0);
            HttpRequest httpRequest = jexlHttpRequestMapper.map(mySinkRecord);
            assertThat(httpRequest.getUrl()).isEqualTo("http://test.com");
            assertThat(httpRequest.getMethod()).isEqualTo(HttpRequest.Method.PUT);
            assertThat(httpRequest.getBodyType()).isEqualTo(HttpRequest.BodyType.STRING);
            assertThat(httpRequest.getBodyAsString()).isEqualTo("mybodycontent");
        }

    }

    private String getDummyHttpRequestAsString() {
        return "{\n" +
                "  \"url\": \"" + DUMMY_URL + "\",\n" +
                "  \"headers\": {},\n" +
                "  \"method\": \"" + DUMMY_METHOD + "\",\n" +
                "  \"bodyAsString\": \"" + DUMMY_BODY + "\",\n" +
                "  \"bodyAsByteArray\": [],\n" +
                "  \"bodyAsForm\": {},\n" +
                "  \"bodyAsMultipart\": [],\n" +
                "  \"bodyType\": \"" + DUMMY_BODY_TYPE + "\"\n" +
                "}";
    }
}