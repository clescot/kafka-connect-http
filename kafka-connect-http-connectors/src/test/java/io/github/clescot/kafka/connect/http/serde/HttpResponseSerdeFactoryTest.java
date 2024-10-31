package io.github.clescot.kafka.connect.http.serde;

import com.google.common.collect.Maps;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

class HttpResponseSerdeFactoryTest {
    @Nested
    class Constructor{
        @Test
        void test_both_args_are_null(){
            Assertions.assertThrows(NullPointerException.class,()-> new HttpResponseSerdeFactory(null, null));
        }
        @Test
        void test_schema_registry_client_is_null(){
            HashMap<String, Object> serdeConfig = Maps.newHashMap();
            Assertions.assertThrows(NullPointerException.class,()-> new HttpResponseSerdeFactory(null, serdeConfig));
        }
        @Test
        void test_serde_config_is_null(){
            try(MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient()) {
                Assertions.assertThrows(NullPointerException.class, () -> new HttpResponseSerdeFactory(schemaRegistryClient, null));
            }catch (IOException e){
                throw new RuntimeException(e);
            }
        }
    }

    @Nested
    class BuildSerde{
        private HttpResponseSerdeFactory httpResponseSerdeFactory;
        @BeforeEach
        public void setup(){
            SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
            HashMap<String, Object> serdeConfig = Maps.newHashMap();
            serdeConfig.put("schema.registry.url","http://fake.com");
            httpResponseSerdeFactory = new HttpResponseSerdeFactory(schemaRegistryClient, serdeConfig);
        }
        @Test
        void test_false(){
            Serde<HttpResponse> httpExchangeSerde = httpResponseSerdeFactory.buildSerde(false);
            assertThat(httpExchangeSerde).isNotNull();
        }
        @Test
        void test_true(){
            Serde<HttpResponse> httpExchangeSerde = httpResponseSerdeFactory.buildSerde(true);
            assertThat(httpExchangeSerde).isNotNull();
        }
    }
}