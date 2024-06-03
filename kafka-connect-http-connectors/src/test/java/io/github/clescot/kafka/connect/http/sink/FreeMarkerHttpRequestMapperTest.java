package io.github.clescot.kafka.connect.http.sink;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import freemarker.template.Configuration;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class FreeMarkerHttpRequestMapperTest {

    private final Map<String, String> templates = Maps.newHashMap();
    private FreeMarkerHttpRequestMapper httpRequestMapper;
    private final static Configuration CONFIGURATION = new Configuration(Configuration.VERSION_2_3_32);

    @Nested
    class Constructor {
        @Test
        void test_no_templates() {
            Assertions.assertThrows(IllegalArgumentException.class, () -> httpRequestMapper = new FreeMarkerHttpRequestMapper(CONFIGURATION, "true", templates));
        }
        @Test
        void test_one_template() {
            templates.put("selector","true");
            Assertions.assertDoesNotThrow(() -> httpRequestMapper = new FreeMarkerHttpRequestMapper(CONFIGURATION, "true", templates));
        }

    }

    @Nested
    class Matches {
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
                    getDummyHttpRequestAsString(),
                    -1,
                    System.currentTimeMillis(),
                    TimestampType.CREATE_TIME,
                    headers
            );

        }

        @Test
        void test_null_record(){
            templates.put("fake","stuff");
            httpRequestMapper = new FreeMarkerHttpRequestMapper(CONFIGURATION, "true", templates);
            Assertions.assertThrows(NullPointerException.class,()-> httpRequestMapper.matches(null));
        }
        @Test
        void test_true_selector_template(){
            templates.put("cd","sdqqsd");
            httpRequestMapper = new FreeMarkerHttpRequestMapper(CONFIGURATION, "true", templates);
            boolean matches = httpRequestMapper.matches(sinkRecord);
            assertThat(matches).isTrue();
        }
        @Test
        void test_false_selector_template(){
            templates.put("ab","qdqd");
            httpRequestMapper = new FreeMarkerHttpRequestMapper(CONFIGURATION, "false", templates);
            boolean matches = httpRequestMapper.matches(sinkRecord);
            assertThat(matches).isFalse();
        }

        @Test
        void test_selector_template_based_on_record(){
            templates.put("qqq","ddddd");
            httpRequestMapper = new FreeMarkerHttpRequestMapper(CONFIGURATION, "'${sinkRecord.topic()}'=='myTopic'", templates);
            boolean matches = httpRequestMapper.matches(sinkRecord);
            assertThat(matches).isTrue();
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
}