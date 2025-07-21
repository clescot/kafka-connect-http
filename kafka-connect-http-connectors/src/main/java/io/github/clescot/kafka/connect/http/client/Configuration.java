package io.github.clescot.kafka.connect.http.client;


public interface Configuration<C,R> {

    boolean matches(R request);

    C getClient();
}
