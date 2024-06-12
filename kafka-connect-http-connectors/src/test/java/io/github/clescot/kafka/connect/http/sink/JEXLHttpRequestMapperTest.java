package io.github.clescot.kafka.connect.http.sink;

import com.google.common.collect.Lists;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import org.apache.kafka.common.record.TimestampType;
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
    @Nested
    class Constructor {


        @Test
        void test_empty_expression() {
            Assertions.assertThrows(IllegalArgumentException.class,()->new JEXLHttpRequestMapper("","",null,null));
        }
        @Test
        void test_null_expression() {
            Assertions.assertThrows(NullPointerException.class,()->new JEXLHttpRequestMapper(null,"'http://url.com'",null,null));
        }

        @Test
        void test_valid_expression_with_url_as_constant() {
            Assertions.assertDoesNotThrow(()->new JEXLHttpRequestMapper("sinkRecord.topic()=='myTopic'","'http://url.com'",null,null));
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
            JEXLHttpRequestMapper httpRequestMapper = new JEXLHttpRequestMapper("sinkRecord.topic()=='myTopic'","'http://url.com'",null,null);
            boolean matches = httpRequestMapper.matches(sinkRecord);
            assertThat(matches).isTrue();
        }

        @Test
        void test_invalid_expression() {
            JEXLHttpRequestMapper httpRequestMapper = new JEXLHttpRequestMapper("toto.topic()=='myTopic'","'http://url.com'",null,null);
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
            JEXLHttpRequestMapper httpRequestMapper = new JEXLHttpRequestMapper("sinkRecord.topic()=='myTopic'","'http://url.com'",null,null);
            boolean matches = httpRequestMapper.matches(sinkRecord);
            assertThat(matches).isTrue();
            HttpRequest httpRequest = httpRequestMapper.map(sinkRecord);
            assertThat(httpRequest.getUrl()).isEqualTo("http://url.com");
            assertThat(httpRequest.getMethod()).isEqualTo("GET");
            assertThat(httpRequest.getBodyType()).isEqualTo(HttpRequest.BodyType.STRING);
        }
        @Test
        void test_url_as_variable(){
            JEXLHttpRequestMapper httpRequestMapper = new JEXLHttpRequestMapper("sinkRecord.topic()=='myTopic'","sinkRecord.value()",null,null);
            boolean matches = httpRequestMapper.matches(sinkRecord);
            assertThat(matches).isTrue();
            HttpRequest httpRequest = httpRequestMapper.map(sinkRecord);
            assertThat(httpRequest.getUrl()).isEqualTo("http://test.com");
            assertThat(httpRequest.getMethod()).isEqualTo("GET");
            assertThat(httpRequest.getBodyType()).isEqualTo(HttpRequest.BodyType.STRING);
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