package io.github.clescot.kafka.connect.http.serde;

import io.github.clescot.kafka.connect.http.core.JsonStringSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonStringSerdeFactory<T> implements SerdeFactory<T> {

    private final Serializer<T> serializer;
    private final Deserializer<T> deserializer;

    public JsonStringSerdeFactory(Serializer<T> serializer, Deserializer<T> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Override
    public Serde<T> buildSerde(boolean recordKey) {
        return new JsonStringSerde<>(serializer,deserializer);
    }
}
