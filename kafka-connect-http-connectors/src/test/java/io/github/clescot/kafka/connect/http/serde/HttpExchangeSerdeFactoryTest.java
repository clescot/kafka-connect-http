package io.github.clescot.kafka.connect.http.serde;

import com.google.common.collect.Maps;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;


class HttpExchangeSerdeFactoryTest {


    @Nested
    class Constructor{
        @Test
        void test_both_args_are_null(){
            Assertions.assertThrows(NullPointerException.class,()-> new HttpExchangeSerdeFactory(null, null));
        }
        @Test
        void test_schema_registry_client_is_null(){
            HashMap<String, Object> serdeConfig = Maps.newHashMap();
            Assertions.assertThrows(NullPointerException.class,()-> new HttpExchangeSerdeFactory(null, serdeConfig));
        }
        @Test
        void test_serde_config_is_null(){
            try(MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient()) {
                Assertions.assertThrows(NullPointerException.class, () -> new HttpExchangeSerdeFactory(schemaRegistryClient, null));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Nested
    class BuildSerde{
        private HttpExchangeSerdeFactory httpExchangeSerdeFactory;
        @BeforeEach
        void setup(){
            SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
            HashMap<String, Object> serdeConfig = Maps.newHashMap();
            serdeConfig.put("schema.registry.url","http://fake.com");
            httpExchangeSerdeFactory = new HttpExchangeSerdeFactory(schemaRegistryClient, serdeConfig);
        }
        @Test
        void test_false(){
            Serde<HttpExchange> httpExchangeSerde = httpExchangeSerdeFactory.buildSerde(false);
            assertThat(httpExchangeSerde).isNotNull();
        }
        @Test
        void test_true(){
            Serde<HttpExchange> httpExchangeSerde = httpExchangeSerdeFactory.buildSerde(true);
            assertThat(httpExchangeSerde).isNotNull();
        }
    }


}