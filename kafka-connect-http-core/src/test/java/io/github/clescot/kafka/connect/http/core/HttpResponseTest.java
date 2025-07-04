package io.github.clescot.kafka.connect.http.core;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.SpecificationVersion;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class HttpResponseTest {

    private KafkaJsonSchemaSerializer<HttpResponse> serializer;

    @BeforeEach
    public void setup(){
        SpecificationVersion jsonSchemaSpecification = SpecificationVersion.DRAFT_2019_09;
        Map<String,String> jsonSchemaSerializerConfig = Maps.newHashMap();
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,"mock://stuff.com");
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION,jsonSchemaSpecification.toString());
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601,"true");
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES,""+false);
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.FAIL_INVALID_SCHEMA,""+true);
        jsonSchemaSerializerConfig.put(KafkaJsonSchemaSerializerConfig.FAIL_UNKNOWN_PROPERTIES,""+true);

        MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient(Lists.newArrayList(new JsonSchemaProvider()));

        serializer = new KafkaJsonSchemaSerializer<>(schemaRegistryClient,jsonSchemaSerializerConfig);
    }

    @Nested
    class TestSerialize{
        @Test
        public void test_serialize_empty_http_response(){
            HttpResponse httpResponse = new HttpResponse();
            //required fields are missing
            Assertions.assertThrows(SerializationException.class,()->serializer.serialize("dummy_topic",httpResponse));
        }

        @Test
        public void test_serialize_http_response_with_required_fields(){
            HttpResponse httpResponse = new HttpResponse();
            httpResponse.setStatusCode(200);
            httpResponse.setStatusMessage("OK");
            //required fields are missing
            byte[] bytes = serializer.serialize("dummy_topic", httpResponse);
            assertThat(bytes).isNotEmpty();
        }
    }

    @Nested
    class TestClone{
        @Test
        public void test_clone_http_response() {
            HttpResponse httpResponse = new HttpResponse(200,"OK");
            httpResponse.setBodyAsString("Hello World");

            HttpResponse cloned = httpResponse.clone();

            assertThat(cloned).isNotSameAs(httpResponse);
            assertThat(cloned.getStatusCode()).isEqualTo(httpResponse.getStatusCode());
            assertThat(cloned.getStatusMessage()).isEqualTo(httpResponse.getStatusMessage());
            assertThat(cloned.getHeaders()).containsAllEntriesOf(httpResponse.getHeaders());
            assertThat(cloned.getBodyType()).isEqualTo(httpResponse.getBodyType());
            assertThat(cloned.getBodyAsString()).isEqualTo(httpResponse.getBodyAsString());
            assertThat(cloned.getBodyAsByteArray()).isEqualTo(httpResponse.getBodyAsByteArray());
            assertThat(cloned.getBodyAsForm()).isEqualTo(httpResponse.getBodyAsForm());
        }
    }
}