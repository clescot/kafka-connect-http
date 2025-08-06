package io.github.clescot.kafka.connect.http.core;

public enum BodyType {
    STRING,
    BYTE_ARRAY,
    FORM,
    MULTIPART;

    @Override
    public String toString() {
        return name();
    }
}
