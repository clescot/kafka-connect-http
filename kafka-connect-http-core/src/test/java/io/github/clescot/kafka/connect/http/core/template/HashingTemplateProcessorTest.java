package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class HashingTemplateProcessorTest {

    private HashingTemplateProcessor processor;
    private HttpExchange testExchange;

    @BeforeEach
    void setUp() {
        processor = new HashingTemplateProcessor();
        
        HttpRequest request = new HttpRequest("http://example.com/api/test", HttpRequest.Method.GET);
        HttpResponse response = new HttpResponse(200, "OK");
        
        testExchange = new HttpExchange(
                request,
                response,
                100L,
                OffsetDateTime.now(),
                new AtomicInteger(1),
                true
        );
    }

    @Test
    void testGetName() {
        assertThat(processor.getName()).isEqualTo("hash");
    }

    @Test
    void testSupportsValidTemplates() {
        assertThat(processor.supports("${hash:MD5:test}")).isTrue();
        assertThat(processor.supports("${hash:SHA-256:input}")).isTrue();
        assertThat(processor.supports("${hash:SHA-512:data:result}")).isTrue();
        assertThat(processor.supports("${hash:sha-256:input}")).isTrue();
    }

    @Test
    void testSupportsInvalidTemplates() {
        assertThat(processor.supports("${jsonpath:$.test}")).isFalse();
        assertThat(processor.supports("${hash:invalid_alg:data}")).isFalse();
        assertThat(processor.supports("${hash:}")).isFalse();
        assertThat(processor.supports("${hash:MD5}")).isTrue();
        assertThat(processor.supports("${hash:MD5}")).isTrue();
        assertThat(processor.supports("${hash}")).isFalse();
        assertThat(processor.supports("plain text")).isFalse();
        assertThat(processor.supports("${hash:unsupported:data}")).isFalse();
    }

    @Test
    void testSupportsAllSupportedAlgorithms() {
        assertThat(processor.supports("${hash:MD5:data}")).isTrue();
        assertThat(processor.supports("${hash:SHA-1:data}")).isTrue();
        assertThat(processor.supports("${hash:SHA-256:data}")).isTrue();
        assertThat(processor.supports("${hash:SHA-384:data}")).isTrue();
        assertThat(processor.supports("${hash:SHA-512:data}")).isTrue();
        assertThat(processor.supports("${hash:SHA3-256:data}")).isTrue();
        assertThat(processor.supports("${hash:SHA3-384:data}")).isTrue();
        assertThat(processor.supports("${hash:SHA3-512:data}")).isTrue();
    }

    @Test
    void testProcessWithMD5() throws Exception {
        String template = "${hash:MD5:hello}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        
        assertThat(httpProcessedExchange.getAttributes()).containsKey("hash_result");
        String hash = httpProcessedExchange.getAttributes().get("hash_result").toString();
        
        String expectedHash = hashString("hello", "MD5");
        assertThat(hash).isEqualTo(expectedHash);
        assertThat(hash).hasSize(32);
    }

    @Test
    void testProcessWithSHA256() throws Exception {
        String template = "${hash:SHA-256:test data}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String hash = httpProcessedExchange.getAttributes().get("hash_result").toString();
        
        String expectedHash = hashString("test data", "SHA-256");
        assertThat(hash).isEqualTo(expectedHash);
        assertThat(hash).hasSize(64);
    }

    @Test
    void testProcessWithSHA512() throws Exception {
        String template = "${hash:SHA-512:input}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String hash = httpProcessedExchange.getAttributes().get("hash_result").toString();
        
        String expectedHash = hashString("input", "SHA-512");
        assertThat(hash).isEqualTo(expectedHash);
        assertThat(hash).hasSize(128);
    }

    @Test
    void testProcessWithCustomAttributeName() {
        String template = "${hash:MD5:secret:my_hash}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("my_hash");
        assertThat(httpProcessedExchange.getAttributes().get("my_hash").toString()).isNotEmpty();
    }

    @Test
    void testProcessWithAttributeReference() throws Exception {
        HttpExchange exchangeWithAttr = (HttpExchange) testExchange.withAttribute("data_to_hash", "password123");
        
        String template = "${hash:MD5:@data_to_hash}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithAttr, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String hash = httpProcessedExchange.getAttributes().get("hash_result").toString();
        
        String expectedHash = hashString("password123", "MD5");
        assertThat(hash).isEqualTo(expectedHash);
    }

    @Test
    void testProcessWithMissingAttribute() {
        String template = "${hash:MD5:@missing_attr}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("hash_result");
        assertThat(httpProcessedExchange.getAttributes().get("hash_result").toString()).isEmpty();
    }

    @Test
    void testProcessWithEmptyInput() {
        String template = "${hash:MD5:}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("hash_result");
        assertThat(httpProcessedExchange.getAttributes().get("hash_result").toString()).isEmpty();
    }

    @Test
    void testProcessWithUnsupportedAlgorithm() {
        String template = "${hash:UNSUPPORTED:data}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("hash_result");
        assertThat(httpProcessedExchange.getAttributes().get("hash_result").toString()).isEmpty();
    }

    @Test
    void testProcessPreservesExistingAttributes() {
        HttpExchange exchangeWithAttr = (HttpExchange) testExchange.withAttribute("existing", "value");
        
        String template = "${hash:MD5:test}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithAttr, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("existing");
        assertThat(httpProcessedExchange.getAttributes().get("existing").toString()).isEqualTo("value");
        assertThat(httpProcessedExchange.getAttributes()).containsKey("hash_result");
    }

    @Test
    void testProcessWithCaseInsensitiveAlgorithm() {
        String templateLower = "${hash:md5:test}";
        String templateUpper = "${hash:MD5:test}";
        
        Exchange<?, ?> processedLower = processor.process(testExchange, templateLower, new HashMap<>());
        Exchange<?, ?> processedUpper = processor.process(testExchange, templateUpper, new HashMap<>());
        
        String hashLower = ((HttpExchange) processedLower).getAttributes().get("hash_result").toString();
        String hashUpper = ((HttpExchange) processedUpper).getAttributes().get("hash_result").toString();
        
        assertThat(hashLower).isEqualTo(hashUpper);
    }

    @Test
    void testProcessWithSpecialCharacters() throws Exception {
        String template = "${hash:MD5:hello world!@#$%}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String hash = httpProcessedExchange.getAttributes().get("hash_result").toString();
        
        String expectedHash = hashString("hello world!@#$%", "MD5");
        assertThat(hash).isEqualTo(expectedHash);
    }

    @Test
    void testProcessWithUnicodeCharacters() throws Exception {
        String template = "${hash:MD5:日本語テスト}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String hash = httpProcessedExchange.getAttributes().get("hash_result").toString();
        
        String expectedHash = hashString("日本語テスト", "MD5");
        assertThat(hash).isEqualTo(expectedHash);
    }

    @Test
    void testProcessWithLongInput() throws Exception {
        StringBuilder longInput = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longInput.append("abcdefghij");
        }
        
        String template = "${hash:MD5:" + longInput.toString() + "}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String hash = httpProcessedExchange.getAttributes().get("hash_result").toString();
        
        assertThat(hash).hasSize(32);
        assertThat(hash).matches("[0-9a-f]+");
    }

    @Test
    void testProcessWithSHA3Algorithms() throws Exception {
        String template = "${hash:SHA3-256:test}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String hash = httpProcessedExchange.getAttributes().get("hash_result").toString();
        
        assertThat(hash).hasSize(64);
        assertThat(hash).matches("[0-9a-f]+");
    }

    @Test
    void testProcessWithSHA384() {
        String template = "${hash:SHA-384:test}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String hash = httpProcessedExchange.getAttributes().get("hash_result").toString();
        
        assertThat(hash).hasSize(96);
        assertThat(hash).matches("[0-9a-f]+");
    }

    @Test
    void testProcessWithEmptyAttributeName() {
        String template = "${hash:MD5:test:}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
    }

    @Test
    void testProcessWithMalformedTemplate() {
        String template = "${hash:test";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());
        
        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
    }

    @Test
    void testProcessWithAttributeValueObject() throws Exception {
        HttpExchange exchangeWithAttr = (HttpExchange) testExchange.withAttribute("number", 12345);
        
        String template = "${hash:MD5:@number}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithAttr, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String hash = httpProcessedExchange.getAttributes().get("hash_result").toString();
        
        String expectedHash = hashString("12345", "MD5");
        assertThat(hash).isEqualTo(expectedHash);
    }

    @Test
    void testProcessWithNullAttributeValue() {
        java.util.Map<String, Object> attrs = new java.util.HashMap<>();
        attrs.put("null_attr", null);
        
        HttpExchange exchangeWithAttrs = HttpExchange.Builder.anHttpExchange()
                .withHttpRequest(testExchange.getRequest())
                .withHttpResponse(testExchange.getResponse())
                .withDuration(testExchange.getDurationInMillis())
                .at(testExchange.getMoment())
                .withAttempts(testExchange.getAttempts())
                .withAttributes(attrs)
                .build();
        
        String template = "${hash:MD5:@null_attr}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithAttrs, template, new HashMap<>());
        
        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes().get("hash_result").toString()).isEmpty();
    }

    private String hashString(String input, String algorithm) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance(algorithm);
        byte[] hashBytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
        BigInteger number = new BigInteger(1, hashBytes);
        String hexString = number.toString(16);
        int expectedLength = hashBytes.length * 2;
        StringBuilder paddedHex = new StringBuilder(hexString);
        while (paddedHex.length() < expectedLength) {
            paddedHex.insert(0, '0');
        }
        return paddedHex.toString();
    }
}
