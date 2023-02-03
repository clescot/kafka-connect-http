package io.github.clescot.kafka.connect.http.sink.model;


import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.json.SpecificationVersion;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.clescot.kafka.connect.http.sink.client.ahc.AHCHttpClient.SUCCESS;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;


public class HttpExchangeTest {

        private HttpRequest getDummyHttpRequest(){
            HttpRequest httpRequest = new HttpRequest(
                    "http://www.toto.com", "GET", "STRING");
            httpRequest.setBodyAsString("stuff");
            return httpRequest;
        }
        private HttpResponse getDummyHttpResponse(int statusCode){
            HttpResponse httpResponse = new HttpResponse(
                    statusCode, "OK");
            httpResponse.setResponseBody("nfgnlksdfnlnskdfnlsf");
            return httpResponse;
        }
        @Test
        public void test_nominal_case() {
            HttpExchange httpExchange = new HttpExchange(
                    getDummyHttpRequest(),
                    getDummyHttpResponse(200),
                    100,
                    OffsetDateTime.now(),
                    new AtomicInteger(2),
                    SUCCESS);
            HttpExchange httpExchange1 = new HttpExchange(
                    getDummyHttpRequest(),
                  getDummyHttpResponse(200),
                    100,
                    OffsetDateTime.now(),
                    new AtomicInteger(2),
                    SUCCESS);
            httpExchange1.equals(httpExchange);
        }

        @Test
        public void test_nominal_case_detail() {
            int statusCode = 404;
            String responseBody = "nfgnlksdfnlnskdfnlsf";
            HttpExchange httpExchange = new HttpExchange(
                    getDummyHttpRequest(),
                   getDummyHttpResponse(statusCode),
                    745L,
                    OffsetDateTime.now(),
                    new AtomicInteger(2),
                    SUCCESS
            );
            assertThat(httpExchange.getHttpResponse().getResponseBody()).isEqualTo(responseBody);
            assertThat(httpExchange.getHttpResponse().getStatusCode()).isEqualTo(statusCode);
        }
        @Test
        public void generate_json_schema() throws IOException {
            int statusCode = 200;
            HttpExchange httpExchange = new HttpExchange(
                    getDummyHttpRequest(),
                    getDummyHttpResponse(statusCode),
                    745L,
                    OffsetDateTime.now(),
                    new AtomicInteger(2),
                    SUCCESS
            );

            //get JSON schema
            SpecificationVersion jsonSchemaSpecification = SpecificationVersion.DRAFT_2019_09;
            boolean useOneOfForNullables = false;
            boolean failUnknownProperties = true;
            JsonSchema expectedJsonSchema = JsonSchemaUtils.getSchema(
                    httpExchange,
                    jsonSchemaSpecification,
                    useOneOfForNullables,
                    failUnknownProperties,
                    null
            );
            System.out.println(expectedJsonSchema);
        }
}

