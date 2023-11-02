package io.github.clescot.kafka.connect.http.sink;

public enum PublishMode {
    IN_MEMORY_QUEUE,
    DLQ,
    PRODUCER,
    NONE
}
