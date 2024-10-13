package io.github.clescot.kafka.connect.http.serde;

import org.apache.kafka.common.serialization.Serde;

public interface SerdeFactory<T> {

    public Serde<T> buildSerde(boolean recordKey);
}
