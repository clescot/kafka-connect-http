package io.github.clescot.kafka.connect.http.sink;


import com.google.common.collect.Maps;
import freemarker.template.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;

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
}