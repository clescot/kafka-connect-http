package io.github.clescot.kafka.connect.http.core;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class HttpExchangeSerializerTest {

    @Test
    void serialize(){
        HttpRequest httpRequest = new HttpRequest();
        HttpResponse httpResponse= new HttpResponse();
        HttpExchange httpExchange = new HttpExchange(httpRequest,httpResponse,100, OffsetDateTime.now(),new AtomicInteger(1),true);
        HttpExchangeSerializer httpExchangeSerializer = new HttpExchangeSerializer();
        byte[] serialized = httpExchangeSerializer.serialize("dummy", httpExchange);
        assertThat(serialized).isNotNull();
    }

}