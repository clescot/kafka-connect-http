package io.github.clescot.kafka.connect.sse.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class SseEventTest {

    @Nested
    class SerializationAndDeserialization {
        private ObjectMapper objectMapper = new ObjectMapper();

        @Nested
        class Serialization {


            // Example test
            @Test
            void shouldSerializeSseEvent() throws JsonProcessingException {
                SseEvent event = new SseEvent("1", "message", "Hello, World!");
                assertNotNull(event);
                assertEquals("1", event.getId());
                assertEquals("message", event.getType());
                assertEquals("Hello, World!", event.getData());
                String serializedEvent = objectMapper.writeValueAsString(event);
                assertThat(serializedEvent).isEqualTo("{\"attributes\":{},\"id\":\"1\",\"type\":\"message\",\"data\":\"Hello, World!\"}");
            }
        }

        @Nested
        class Deserialization {
            @Test
            void shouldDeserializeSseEvent() throws JsonProcessingException {
                String json = "{\"id\":\"1\",\"type\":\"message\",\"data\":\"Hello, World!\"}";
                SseEvent event = objectMapper.readValue(json, SseEvent.class);
                assertNotNull(event);
                assertEquals("1", event.getId());
                assertEquals("message", event.getType());
                assertEquals("Hello, World!", event.getData());
            }
        }
    }

    @Nested
    class EqualityAndHashCode {

        @Test
        void shouldBeEqual() {
            SseEvent event1 = new SseEvent("1", "message", "Hello, World!");
            SseEvent event2 = new SseEvent("1", "message", "Hello, World!");
            assertEquals(event1, event2);
            assertEquals(event1.hashCode(), event2.hashCode());
        }

        @Test
        void shouldNotBeEqual() {
            SseEvent event1 = new SseEvent("1", "message", "Hello, World!");
            SseEvent event2 = new SseEvent("2", "message", "Hello, World!");
            assertNotEquals(event1, event2);
        }
    }

    @Nested
    class Clone {

        @Test
        void shouldCloneSseEvent() {
            SseEvent original = new SseEvent("1", "message", "Hello, World!");
            SseEvent clone = original.clone();
            assertNotSame(original, clone);
            assertEquals(original, clone);
        }
    }

    @Nested
    class ToStringAndJson {

        @Test
        void shouldReturnCorrectToString() {
            SseEvent event = new SseEvent("1", "message", "Hello, World!");
            String expected = "SseEvent{id='1', attributes='{}', type='message', data='Hello, World!'}";
            assertEquals(expected, event.toString());
        }

        @Test
        void shouldReturnCorrectJson() {
            SseEvent event = new SseEvent("1", "message", "Hello, World!");
            String expectedJson = "{\"id\":\"1\",\"attributes\":\"{}\",\"type\":\"message\",\"data\":\"Hello, World!\"}";
            assertEquals(expectedJson, event.toJson());
        }
    }

    @Nested
    class ToStruct {

        @Test
        void shouldConvertToStruct() {
            SseEvent event = new SseEvent("1", "message", "Hello, World!");
            Struct struct = event.toStruct();
            assertNotNull(struct);
            assertEquals("1", struct.getString("id"));
            assertEquals("message", struct.getString("type"));
            assertEquals("Hello, World!", struct.getString("data"));
        }
    }

    @Nested
    class Constructor {

        @Test
        void shouldCreateSseEventWithAllFields() {
            SseEvent event = new SseEvent("1", "message", "Hello, World!");
            assertNotNull(event);
            assertEquals("1", event.getId());
            assertEquals("message", event.getType());
            assertEquals("Hello, World!", event.getData());
        }

        @Test
        void shouldCreateSseEventWithEmptyFields() {
            SseEvent event = new SseEvent("", "", "");
            assertNotNull(event);
            assertEquals("", event.getId());
            assertEquals("", event.getType());
            assertEquals("", event.getData());
        }

        @Test
        void shouldCreateSseEventWithNullFields() {
            SseEvent event = new SseEvent(null, null, null);
            assertNotNull(event);
            assertNull(event.getId());
            assertNull(event.getType());
            assertNull(event.getData());
        }

        @Test
        void shoudlCreateSseEventWithStruct() {
                Struct struct = new Struct(SseEvent.SCHEMA)
                        .put("id", "1")
                        .put("type", "message")
                        .put("data", "Hello, World!");
                SseEvent event = new SseEvent(struct);
                assertNotNull(event);
                assertEquals("1", event.getId());
                assertEquals("message", event.getType());
                assertEquals("Hello, World!", event.getData());
        }
    }

}