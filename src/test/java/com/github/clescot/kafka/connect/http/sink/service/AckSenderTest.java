package com.github.clescot.kafka.connect.http.sink.service;


import com.github.clescot.kafka.connect.http.sink.model.Acknowledgement;
import com.github.clescot.kafka.connect.http.sink.config.AckConfig;
import com.github.clescot.kafka.connect.http.sink.config.ConfigConstants;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(Enclosed.class)
public class AckSenderTest {

    public static class Test_convert_value {

        public static final String FAKE_RESPONSE = "fake response";

        @Before
        public void setUp(){
            AckSender.clearCurrentInstance();
        }

        @Test
        public void test_nominal_case() throws URISyntaxException, IOException {
            //given
            HashMap<Object, Object> config = Maps.newHashMap();
            config.put(ConfigConstants.TARGET_BOOTSTRAP_SERVER, "localhost:9092");
            config.put(ConfigConstants.TARGET_SCHEMA_REGISTRY, "http://localhost:8081");
            config.put(ConfigConstants.PRODUCER_CLIENT_ID, "fake.client.id");
            config.put(ConfigConstants.ACK_TOPIC, "fake.ack.topic");
            URL resource = Thread.currentThread().getContextClassLoader().getResource("ack-schema.json");
            String content = new String(Files.readAllBytes(Paths.get(resource.toURI())));
            config.put(ConfigConstants.ACK_SCHEMA, content);
            AckConfig ackConfig = new AckConfig(config);

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "producerId");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "fake.schema.registry:8081");
            KafkaFailSafeProducer<String, byte[]> producer = new KafkaFailSafeProducer(()->new KafkaProducer(props),2);

            AvroConverter keyAvroConverter = mock(AvroConverter.class);

            AvroConverter valueAvroConverter = mock(AvroConverter.class);
            when(valueAvroConverter.fromConnectData(anyString(), any(Schema.class), any())).thenReturn(FAKE_RESPONSE.getBytes());
            AckSender ackSender = AckSender.getInstance(ackConfig, producer, keyAvroConverter, valueAvroConverter);

            Acknowledgement acknowledgement = new Acknowledgement("", "sd897osdmsdg", 200, "toto", Lists.newArrayList(), "nfgnlksdfnlnskdfnlsf","http://toto:8081",Lists.newArrayList(),"PUT","", 100L, OffsetDateTime.now(), new AtomicInteger(2));
            //when
            byte[] result = ackSender.convertValue(acknowledgement);
            //then
            assertThat(result).isEqualTo("fake response".getBytes());
        }

        @Test(expected = NullPointerException.class)
        public void test_null() throws URISyntaxException, IOException {
            HashMap<Object, Object> config = Maps.newHashMap();
            config.put(ConfigConstants.TARGET_BOOTSTRAP_SERVER, "localhost:9092");
            config.put(ConfigConstants.TARGET_SCHEMA_REGISTRY, "http://localhost:8081");
            config.put(ConfigConstants.PRODUCER_CLIENT_ID, "fake.client.id");
            config.put(ConfigConstants.ACK_TOPIC, "fake.ack.topic");
            URL resource = Thread.currentThread().getContextClassLoader().getResource("ack-schema.json");
            String content = new String(Files.readAllBytes(Paths.get(resource.toURI())));
            config.put(ConfigConstants.ACK_SCHEMA, content);
            AckConfig ackConfig = new AckConfig(config);

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "producerId");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "fake.schema.registry:8081");
            KafkaFailSafeProducer<String, byte[]> producer = new KafkaFailSafeProducer(()->new KafkaProducer(props),2);
            AvroConverter keyAvroConverter = getAvroConverter(true);
            AvroConverter valueAvroConverter = getAvroConverter(false);
            AckSender ackSender = AckSender.getInstance(ackConfig, producer, keyAvroConverter, valueAvroConverter);
            ackSender.convertValue(null);
        }

        @Test(expected = NullPointerException.class)
        public void test_missing_correlationId() throws URISyntaxException, IOException {
            HashMap<Object, Object> config = Maps.newHashMap();
            config.put(ConfigConstants.TARGET_BOOTSTRAP_SERVER, "localhost:9092");
            config.put(ConfigConstants.TARGET_SCHEMA_REGISTRY, "http://localhost:8081");
            config.put(ConfigConstants.PRODUCER_CLIENT_ID, "fake.client.id");
            config.put(ConfigConstants.ACK_TOPIC, "fake.ack.topic");
            URL resource = Thread.currentThread().getContextClassLoader().getResource("ack-schema.json");
            String content = new String(Files.readAllBytes(Paths.get(resource.toURI())));
            config.put(ConfigConstants.ACK_SCHEMA, content);
            AckConfig ackConfig = new AckConfig(config);

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "producerId");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "fake.schema.registry:8081");
            KafkaFailSafeProducer<String, byte[]> producer = new KafkaFailSafeProducer(()->new KafkaProducer(props),2);
            AvroConverter keyAvroConverter = getAvroConverter(true);
            AvroConverter valueAvroConverter = getAvroConverter(false);
            AckSender ackSender = AckSender.getInstance(ackConfig, producer, keyAvroConverter, valueAvroConverter);
            Acknowledgement acknowledgement = new Acknowledgement(null,null, 200, "toto",Lists.newArrayList(), "nfgnlksdfnlnskdfnlsf","http://toto:8081",Lists.newArrayList(),"POST","",  100L, OffsetDateTime.now(), new AtomicInteger(2));
            ackSender.convertValue(acknowledgement);
        }

        @Test(expected = IllegalStateException.class)
        public void test_missing_status_code() throws URISyntaxException, IOException {
            HashMap<Object, Object> config = Maps.newHashMap();
            config.put(ConfigConstants.TARGET_BOOTSTRAP_SERVER, "localhost:9092");
            config.put(ConfigConstants.TARGET_SCHEMA_REGISTRY, "http://localhost:8081");
            config.put(ConfigConstants.PRODUCER_CLIENT_ID, "fake.client.id");
            config.put(ConfigConstants.ACK_TOPIC, "fake.ack.topic");
            URL resource = Thread.currentThread().getContextClassLoader().getResource("ack-schema.json");
            String content = new String(Files.readAllBytes(Paths.get(resource.toURI())));
            config.put(ConfigConstants.ACK_SCHEMA, content);
            AckConfig ackConfig = new AckConfig(config);

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "producerId");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "fake.schema.registry:8081");
            KafkaFailSafeProducer<String, byte[]> producer = new KafkaFailSafeProducer(()->new KafkaProducer(props),2);
            AvroConverter keyAvroConverter = getAvroConverter(true);
            AvroConverter valueAvroConverter = getAvroConverter(false);
            AckSender ackSender = AckSender.getInstance(ackConfig, producer, keyAvroConverter, valueAvroConverter);
            Acknowledgement acknowledgement = new Acknowledgement("sdfsfd", "sd897osdmsdg", -1, "toto",Lists.newArrayList(), "nfgnlksdfnlnskdfnlsf","http://toto:8081",Lists.newArrayList(),"PUT","",  100L, OffsetDateTime.now(), new AtomicInteger(2));
            ackSender.convertValue(acknowledgement);
        }

        @Test(expected = NullPointerException.class)
        public void test_missing_content() throws URISyntaxException, IOException {
            HashMap<Object, Object> config = Maps.newHashMap();
            config.put(ConfigConstants.TARGET_BOOTSTRAP_SERVER, "localhost:9092");
            config.put(ConfigConstants.TARGET_SCHEMA_REGISTRY, "http://localhost:8081");
            config.put(ConfigConstants.PRODUCER_CLIENT_ID, "fake.client.id");
            config.put(ConfigConstants.ACK_TOPIC, "fake.ack.topic");
            URL resource = Thread.currentThread().getContextClassLoader().getResource("ack-schema.json");
            String content = new String(Files.readAllBytes(Paths.get(resource.toURI())));
            config.put(ConfigConstants.ACK_SCHEMA, content);
            AckConfig ackConfig = new AckConfig(config);

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "producerId");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "fake.schema.registry:8081");
            KafkaFailSafeProducer<String, byte[]> producer = new KafkaFailSafeProducer(()->new KafkaProducer(props),2);
            AvroConverter keyAvroConverter = getAvroConverter(true);
            AvroConverter valueAvroConverter = getAvroConverter(false);
            AckSender ackSender = AckSender.getInstance(ackConfig, producer, keyAvroConverter, valueAvroConverter);
            Acknowledgement acknowledgement = new Acknowledgement( "sdqfsdfsdf","sd897osdmsdg", 200, "toto",Lists.newArrayList(), null,"http://toto:8081",Lists.newArrayList(),"PUT","",  100L, OffsetDateTime.now(), new AtomicInteger(2));
            ackSender.convertValue(acknowledgement);
        }


   
        @Test(expected = NullPointerException.class)
        public void test_missing_status_message() throws URISyntaxException, IOException {
            HashMap<Object, Object> config = Maps.newHashMap();
            config.put(ConfigConstants.TARGET_BOOTSTRAP_SERVER, "localhost:9092");
            config.put(ConfigConstants.TARGET_SCHEMA_REGISTRY, "http://localhost:8081");
            config.put(ConfigConstants.PRODUCER_CLIENT_ID, "fake.client.id");
            config.put(ConfigConstants.ACK_TOPIC, "fake.ack.topic");
            URL resource = Thread.currentThread().getContextClassLoader().getResource("ack-schema.json");
            String content = new String(Files.readAllBytes(Paths.get(resource.toURI())));
            config.put(ConfigConstants.ACK_SCHEMA, content);
            AckConfig ackConfig = new AckConfig(config);

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "producerId");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "fake.schema.registry:8081");
            KafkaFailSafeProducer<String, byte[]> producer = new KafkaFailSafeProducer(()->new KafkaProducer(props),2);
            AvroConverter keyAvroConverter = getAvroConverter(true);
            AvroConverter valueAvroConverter = getAvroConverter(false);
            AckSender ackSender = AckSender.getInstance(ackConfig, producer, keyAvroConverter, valueAvroConverter);
            Acknowledgement acknowledgement = new Acknowledgement("sfsfddf","sd897osdmsdg", 200, null,Lists.newArrayList(), "nfgnlksdfnlnskdfnlsf","http://toto:8081",Lists.newArrayList(),"POST","",  100L, OffsetDateTime.now(), new AtomicInteger(2));
            ackSender.convertValue(acknowledgement);
        }
    }


    public static class Test_send {

        @Test
        public void test_nominal_case() throws URISyntaxException, IOException {
            HashMap<Object, Object> config = Maps.newHashMap();
            config.put(ConfigConstants.TARGET_BOOTSTRAP_SERVER, "localhost:9092");
            config.put(ConfigConstants.TARGET_SCHEMA_REGISTRY, "http://localhost:8081");
            config.put(ConfigConstants.PRODUCER_CLIENT_ID, "fake.client.id");
            config.put(ConfigConstants.ACK_TOPIC, "fake.ack.topic");
            URL resource = Thread.currentThread().getContextClassLoader().getResource("ack-schema.json");
            String content = new String(Files.readAllBytes(Paths.get(resource.toURI())));
            config.put(ConfigConstants.ACK_SCHEMA, content);
            AckConfig ackConfig = new AckConfig(config);
            AckSender.clear();
            KafkaFailSafeProducer<String, byte[]> producer = mock(KafkaFailSafeProducer.class);
            RecordMetadata recordMetadata = new RecordMetadata(null, 1, 1, 1, Long.valueOf(1), 1, 1);
            when(producer.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.supplyAsync(() -> recordMetadata));
            AvroConverter keyAvroConverter = mock(AvroConverter.class);
            when(keyAvroConverter.fromConnectData(anyString(), any(Schema.class), any())).thenReturn("test".getBytes());
            AvroConverter valueAvroConverter = mock(AvroConverter.class);
            AckSender ackSender = AckSender.getInstance(ackConfig, producer, keyAvroConverter, valueAvroConverter);
            ArrayList<Map.Entry<String, String>> responseHeaders = Lists.newArrayList();
            responseHeaders.add(new AbstractMap.SimpleEntry<>("content-type","application/json"));
            responseHeaders.add(new AbstractMap.SimpleEntry<>("toto","titi"));
            ArrayList<Map.Entry<String, String>> requestHeaders = Lists.newArrayList();
            requestHeaders.add(new AbstractMap.SimpleEntry<>("content-type","application/json"));
            requestHeaders.add(new AbstractMap.SimpleEntry<>("tutu","tata"));
            Acknowledgement acknowledgement = new Acknowledgement("fgdfg5","sd897osdmsdg", 200, "toto", responseHeaders, "nfgnlksdfnlnskdfnlsf","http://toto:8081", requestHeaders,"PUT","",  100L, OffsetDateTime.now(), new AtomicInteger(2));
            ackSender.send(acknowledgement);
        }


    }


    private static AvroConverter getAvroConverter(boolean isKey) {
        AvroConverter keyAvroConverter = new AvroConverter();
        keyAvroConverter.configure(ImmutableMap.of(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"), isKey);
        return keyAvroConverter;
    }


    public static class Test_close {

        @Test
        public void test_nominal_case() throws URISyntaxException, IOException {
            HashMap<Object, Object> config = Maps.newHashMap();
            config.put(ConfigConstants.TARGET_BOOTSTRAP_SERVER, "localhost:9092");
            config.put(ConfigConstants.TARGET_SCHEMA_REGISTRY, "http://localhost:8081");
            config.put(ConfigConstants.PRODUCER_CLIENT_ID, "fake.client.id");
            config.put(ConfigConstants.ACK_TOPIC, "fake.ack.topic");
            URL resource = Thread.currentThread().getContextClassLoader().getResource("ack-schema.json");
            String content = new String(Files.readAllBytes(Paths.get(resource.toURI())));
            config.put(ConfigConstants.ACK_SCHEMA, content);
            AckConfig ackConfig = new AckConfig(config);
            AvroConverter keyAvroConverter = getAvroConverter(true);
            AvroConverter valueAvroConverter = getAvroConverter(false);
            KafkaFailSafeProducer<String, byte[]> producer = mock(KafkaFailSafeProducer.class);
            AckSender ackSender = AckSender.getInstance(ackConfig, producer, keyAvroConverter, valueAvroConverter);
            ackSender.close();
        }
    }


}