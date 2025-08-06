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


    public static BodyType getBodyType(String contentType) {
        if (contentType == null || contentType.isEmpty()) {
            return BodyType.STRING;
        }

        if( contentType.startsWith(MediaType.APPLICATION_X_WWW_FORM_URLENCODED)) {
            return BodyType.FORM;
        } else if (contentType.startsWith(MediaType.APPLICATION_OCTET_STREAM)) {
            return BodyType.BYTE_ARRAY;
        } else if (contentType.startsWith(MediaType.MULTIPART_FORM_DATA)) {
            return BodyType.MULTIPART;
        }
        return BodyType.STRING;
    }

}
