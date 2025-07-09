package io.github.clescot.kafka.connect.http.sink;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.github.clescot.kafka.connect.http.core.*;
import io.github.clescot.kafka.connect.http.sink.mapper.DirectHttpRequestMapper;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlFeatures;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.connector.ConnectRecord;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE;
import static io.github.clescot.kafka.connect.http.core.HttpRequest.SCHEMA;
import static io.github.clescot.kafka.connect.http.sink.HttpSinkTask.DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;

class DirectHttpRequestMapperTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectHttpRequestMapperTest.class);
    private static final String DUMMY_BODY = "stuff";
    private static final String DUMMY_URL = "http://www." + DUMMY_BODY + ".com";
    private static final HttpRequest.Method DUMMY_METHOD = HttpRequest.Method.POST;
    private static final String DUMMY_BODY_TYPE = "STRING";
    private DirectHttpRequestMapper httpRequestMapper;
    private SchemaRegistryClient schemaRegistryClient;
    @BeforeEach
    public void setup() throws RestClientException, IOException {
        // Restricted permissions to a safe set but with URI allowed
        JexlPermissions permissions = new JexlPermissions.ClassPermissions(SinkRecord.class, ConnectRecord.class,HttpRequest.class);
        // Create the engine
        JexlFeatures features = new JexlFeatures()
                .loops(false)
                .sideEffectGlobal(false)
                .sideEffect(false);
        JexlEngine jexlEngine = new JexlBuilder().features(features).permissions(permissions).create();
        httpRequestMapper = new DirectHttpRequestMapper(DEFAULT,jexlEngine, "true");
        schemaRegistryClient = new MockSchemaRegistryClient(Lists.newArrayList(new JsonSchemaProvider()));
        //Register http part
        LOGGER.info("Registering schemas in mock schema registry");
        ParsedSchema parsedPartSchema = SchemaLoader.loadHttpPartSchema();
        schemaRegistryClient.register("httpPart",parsedPartSchema);
        //register http request
        ParsedSchema parsedHttpRequestSchema = SchemaLoader.loadHttpRequestSchema();
        schemaRegistryClient.register("httpRequest",parsedHttpRequestSchema);
        //register http response
        ParsedSchema parsedHttpResponseSchema = SchemaLoader.loadHttpResponseSchema();
        schemaRegistryClient.register("httpResponse",parsedHttpResponseSchema);
        //register http exchange
        ParsedSchema parsedHttpExchangeSchema = SchemaLoader.loadHttpExchangeSchema();
        schemaRegistryClient.register("httpExchange",parsedHttpExchangeSchema);
        LOGGER.info("end Registering schemas in mock schema registry");
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
        }

        @Test
        void test_buildHttpRequest_http_request_as_json_schema() {
            //given
            List<Header> headers = Lists.newArrayList();
            HttpRequest dummyHttpRequest = getDummyHttpRequest(DUMMY_URL);
            String topic = "myTopic";


            JsonSchemaConverter jsonSchemaConverter = getJsonSchemaConverter(schemaRegistryClient);


            byte[] httpRequestAsJsonSchemaWithConverter = jsonSchemaConverter.fromConnectData(topic, SCHEMA, ((HttpRequest)dummyHttpRequest.clone()).toStruct());

            SchemaAndValue schemaAndValue = jsonSchemaConverter.toConnectData(topic, httpRequestAsJsonSchemaWithConverter);

            SinkRecord sinkRecord = new SinkRecord(topic, 0, Schema.STRING_SCHEMA, "key", schemaAndValue.schema(), schemaAndValue.value(), -1, System.currentTimeMillis(), TimestampType.CREATE_TIME, headers);
            //when
            HttpRequest httpRequest = httpRequestMapper.map(sinkRecord);
            //then
            assertThat(httpRequest).isNotNull();
            assertThat(httpRequest.getUrl()).isEqualTo(DUMMY_URL);
            assertThat(httpRequest.getMethod()).isEqualTo(DUMMY_METHOD);
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
            ParsedSchema schema1 = parsedSchema.get();
            String canonicalString = schema1.canonicalString();
            mockSchemaRegistryClient.register(subject, schema1);
        } catch (IOException | RestClientException e) {
            throw new RuntimeException(e);
        }
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
        return httpRequest.toStruct();
    }




    @NotNull
    private static HttpRequest getDummyHttpRequest(String url) {
        Map<String, List<String>> headers = Maps.newHashMap();
        headers.put("Content-Type", Lists.newArrayList("application/json"));
        HttpPart httpPart = new HttpPart("stuff");
        Map<String, HttpPart> parts = Maps.newHashMap();
        parts.put("part1", httpPart);
        return new HttpRequest(url, DUMMY_METHOD,headers, HttpRequest.BodyType.MULTIPART, parts);
    }

    private String getDummyHttpRequestAsString() {
        return "{\n" +
                "  \"url\": \"" + DUMMY_URL + "\",\n" +
                "  \"headers\": {},\n" +
                "  \"method\": \"" + DUMMY_METHOD + "\",\n" +
                "  \"bodyAsString\": \"" + DUMMY_BODY + "\",\n" +
                "  \"bodyType\": \"" + DUMMY_BODY_TYPE + "\"\n" +
                "}";
    }



}