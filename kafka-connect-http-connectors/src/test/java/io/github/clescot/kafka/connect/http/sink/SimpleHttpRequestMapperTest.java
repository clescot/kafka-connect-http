package io.github.clescot.kafka.connect.http.sink;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpRequestAsStruct;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE;
import static io.github.clescot.kafka.connect.http.core.HttpRequestAsStruct.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;

class SimpleHttpRequestMapperTest {

    private static final String DUMMY_BODY = "stuff";
    private static final String DUMMY_URL = "http://www." + DUMMY_BODY + ".com";
    private static final String DUMMY_METHOD = "POST";
    private static final String DUMMY_BODY_TYPE = "STRING";
    private SimpleHttpRequestMapper httpRequestMapper;
    @BeforeEach
    public void setup(){
        httpRequestMapper = new SimpleHttpRequestMapper();
    }
    @Nested
    class TestMap {


        @Test
        void test_buildHttpRequest_null_sink_record() {
            //when
            //then
            Assertions.assertThrows(ConnectException.class, () -> httpRequestMapper.map(null));
        }

        @Test
        void test_buildHttpRequest_null_value_sink_record() {
            //when
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, null, -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            //then
            Assertions.assertThrows(ConnectException.class, () -> httpRequestMapper.map(sinkRecord));
        }

        @Test
        void test_buildHttpRequest_http_request_as_string() {
            //given
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, getDummyHttpRequestAsString(), -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            //when
            HttpRequest httpRequest = httpRequestMapper.map(sinkRecord);
            //then
            assertThat(httpRequest).isNotNull();
            assertThat(httpRequest.getUrl()).isEqualTo(DUMMY_URL);
            assertThat(httpRequest.getMethod()).isEqualTo(DUMMY_METHOD);
            assertThat(httpRequest.getBodyType().toString()).hasToString(DUMMY_BODY_TYPE);
        }

        @Test
        void test_buildHttpRequest_http_request_as_json_schema() {
            //given
            List<Header> headers = Lists.newArrayList();
            HttpRequest dummyHttpRequest = getDummyHttpRequest(DUMMY_URL);
            String topic = "myTopic";
            SchemaRegistryClient schemaRegistryClient = getSchemaRegistryClient();
            registerSchema(schemaRegistryClient, topic, 1, 1, HttpRequest.SCHEMA_AS_STRING);


            JsonSchemaConverter jsonSchemaConverter = getJsonSchemaConverter(schemaRegistryClient);


            byte[] httpRequestAsJsonSchemaWithConverter = jsonSchemaConverter.fromConnectData(topic, SCHEMA, new HttpRequestAsStruct(dummyHttpRequest).toStruct());

            SchemaAndValue schemaAndValue = jsonSchemaConverter.toConnectData(topic, httpRequestAsJsonSchemaWithConverter);

            SinkRecord sinkRecord = new SinkRecord(topic, 0, Schema.STRING_SCHEMA, "key", schemaAndValue.schema(), schemaAndValue.value(), -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            //when
            HttpRequest httpRequest = httpRequestMapper.map(sinkRecord);
            //then
            assertThat(httpRequest).isNotNull();
            assertThat(httpRequest.getUrl()).isEqualTo(DUMMY_URL);
            assertThat(httpRequest.getMethod()).isEqualTo(DUMMY_METHOD);
            assertThat(httpRequest.getBodyType().toString()).hasToString(DUMMY_BODY_TYPE);
        }


        @Test
        void test_buildHttpRequest_http_request_as_struct() {
            //given
            List<Header> headers = Lists.newArrayList();
            SinkRecord sinkRecord = new SinkRecord("myTopic", 0, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, getDummyHttpRequestAsStruct(DUMMY_URL), -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            //when
            HttpRequest httpRequest = httpRequestMapper.map(sinkRecord);
            //then
            assertThat(httpRequest).isNotNull();
            assertThat(httpRequest.getUrl()).isEqualTo(DUMMY_URL);
            assertThat(httpRequest.getMethod()).isEqualTo(DUMMY_METHOD);
            assertThat(httpRequest.getBodyType().toString()).hasToString(DUMMY_BODY_TYPE);
        }


    }

    @NotNull
    private static JsonSchemaConverter getJsonSchemaConverter(SchemaRegistryClient mockSchemaRegistryClient) {
        JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter(mockSchemaRegistryClient);
        Map<String, String> config = Maps.newHashMap();
        config.put("schema.registry.url", "http://dummy.com");
        config.put(JSON_VALUE_TYPE, HttpRequest.class.getName());
        jsonSchemaConverter.configure(config, false);
        return jsonSchemaConverter;
    }


    private static void registerSchema(SchemaRegistryClient mockSchemaRegistryClient, String topic, int schemaVersion, int schemaId, String schemaAsString) {
        //we test TopicNameStrategy, and the jsonSchema is owned in the Kafka value record.
        String subject = topic + "-value";
        io.confluent.kafka.schemaregistry.client.rest.entities.Schema schema = new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(subject, schemaVersion, schemaId, JsonSchema.TYPE, Lists.newArrayList(), schemaAsString);
        Optional<ParsedSchema> parsedSchema = mockSchemaRegistryClient.parseSchema(schema);
        try {
            mockSchemaRegistryClient.register(subject, parsedSchema.get());
        } catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    private static SchemaRegistryClient getSchemaRegistryClient() {
        SchemaProvider provider = new JsonSchemaProvider();
        return new MockSchemaRegistryClient(Collections.singletonList(provider));
    }

    private String getIP() {
        try (DatagramSocket datagramSocket = new DatagramSocket()) {
            datagramSocket.connect(InetAddress.getByName("8.8.8.8"), 12345);
            return datagramSocket.getLocalAddress().getHostAddress();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Struct getDummyHttpRequestAsStruct(String url) {
        HttpRequest httpRequest = getDummyHttpRequest(url);
        HttpRequestAsStruct httpRequestAsStruct = new HttpRequestAsStruct(httpRequest);
        return httpRequestAsStruct.toStruct();
    }




    @NotNull
    private static HttpRequest getDummyHttpRequest(String url) {
        HttpRequest httpRequest = new HttpRequest(url, DUMMY_METHOD, DUMMY_BODY_TYPE);
        Map<String, List<String>> headers = Maps.newHashMap();
        headers.put("Content-Type", Lists.newArrayList("application/json"));
        httpRequest.setHeaders(headers);
        httpRequest.setBodyAsString("stuff");
        httpRequest.setBodyAsForm(Maps.newHashMap());
        return httpRequest;
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