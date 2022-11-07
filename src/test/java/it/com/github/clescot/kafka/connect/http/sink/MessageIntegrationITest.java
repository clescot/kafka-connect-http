package it.com.github.clescot.kafka.connect.http.sink;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Properties;


public class MessageIntegrationITest {

    private static final String PRODUCING_TOPIC = "http_sink_response__v1";
    private Producer<String, GenericRecord> producer = buildKafkaProducer();




    @Test
    @Ignore
    public void shouldIgnoreSinkRecord_whenEansFieldIsEmpty() throws Exception {
        //GIVEN
        GenericRecord record = buildGenericRecord();
        ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(PRODUCING_TOPIC, "key", record);

        //WHEN
        producer.send(producerRecord).get();
    }

    private Producer<String, GenericRecord> buildKafkaProducer() {
        Properties props = new Properties();
        String dockerHostAddress = System.getenv("DOCKER_HOST_ADDRESS");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, dockerHostAddress +":9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "multiplier-test-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://"+dockerHostAddress+":8081");

        return new KafkaProducer<>(props);
    }



    private GenericRecord buildGenericRecord() {
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record("g2r").fields();
        fieldAssembler.requiredString("url");
        fieldAssembler.requiredString("httpMethod");
        fieldAssembler.requiredString("body");
        fieldAssembler.requiredString("id");
        Schema schema = fieldAssembler.endRecord();

        GenericRecord record = new GenericData.Record(schema);
        record.put("url", "http://10.63.52.21:86/Grr/IsValide");
        record.put("httpMethod", "PUT");
        record.put("body", "1212");
        record.put("id", "TOTO");
        return record;
    }


}