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
        response.setBodyAsString("original content");

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
        assertThat(processor.supports("${hash:SHA-512:data}")).isTrue();
        assertThat(processor.supports("${hash:sha-256:input}")).isTrue();
    }

    @Test
    void testSupportsInvalidTemplates() {
        assertThat(processor.supports("${jsonpath:$.test}")).isFalse();
        assertThat(processor.supports("${hash:invalid_alg:data}")).isFalse();
        assertThat(processor.supports("${hash:}")).isFalse();
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

        String hash = httpProcessedExchange.getContent();

        String expectedHash = hashString("hello", "MD5");
        assertThat(hash).isEqualTo(expectedHash);
        assertThat(hash).hasSize(32);
    }

    @Test
    void testProcessWithSHA256() throws Exception {
        String template = "${hash:SHA-256:test data}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String hash = httpProcessedExchange.getContent();

        String expectedHash = hashString("test data", "SHA-256");
        assertThat(hash).isEqualTo(expectedHash);
        assertThat(hash).hasSize(64);
    }

    @Test
    void testProcessWithSHA512() throws Exception {
        String template = "${hash:SHA-512:input}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String hash = httpProcessedExchange.getContent();

        String expectedHash = hashString("input", "SHA-512");
        assertThat(hash).isEqualTo(expectedHash);
        assertThat(hash).hasSize(128);
    }

    @Test
    void testProcessWithEmptyInput() {
        String template = "${hash:MD5:}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getContent()).isEmpty();
    }

    @Test
    void testProcessPreservesExistingAttributes() {
        HttpExchange exchangeWithAttr = (HttpExchange) testExchange.withAttribute("existing", "value");

        String template = "${hash:MD5:test}";
        Exchange<?, ?> processedExchange = processor.process(exchangeWithAttr, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        assertThat(httpProcessedExchange.getAttributes()).containsKey("existing");
        assertThat(httpProcessedExchange.getAttributes().get("existing").toString()).isEqualTo("value");
    }

    @Test
    void testProcessWithCaseInsensitiveAlgorithm() {
        String templateLower = "${hash:md5:test}";
        String templateUpper = "${hash:MD5:test}";

        Exchange<?, ?> processedLower = processor.process(testExchange, templateLower, new HashMap<>());
        Exchange<?, ?> processedUpper = processor.process(testExchange, templateUpper, new HashMap<>());

        String hashLower = ((HttpExchange) processedLower).getContent();
        String hashUpper = ((HttpExchange) processedUpper).getContent();

        assertThat(hashLower).isEqualTo(hashUpper);
    }

    @Test
    void testProcessWithSpecialCharacters() throws Exception {
        String template = "${hash:MD5:hello world!@#$%}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String hash = httpProcessedExchange.getContent();

        String expectedHash = hashString("hello world!@#$%", "MD5");
        assertThat(hash).isEqualTo(expectedHash);
    }

    @Test
    void testProcessWithUnicodeCharacters() throws Exception {
        String template = "${hash:MD5:日本語テスト}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String hash = httpProcessedExchange.getContent();

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
        String hash = httpProcessedExchange.getContent();

        assertThat(hash).hasSize(32);
        assertThat(hash).matches("[0-9a-f]+");
    }

    @Test
    void testProcessWithSHA3Algorithms() throws Exception {
        String template = "${hash:SHA3-256:test}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String hash = httpProcessedExchange.getContent();

        assertThat(hash).hasSize(64);
        assertThat(hash).matches("[0-9a-f]+");
    }

    @Test
    void testProcessWithSHA384() {
        String template = "${hash:SHA-384:test}";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        HttpExchange httpProcessedExchange = (HttpExchange) processedExchange;
        String hash = httpProcessedExchange.getContent();

        assertThat(hash).hasSize(96);
        assertThat(hash).matches("[0-9a-f]+");
    }

    @Test
    void testProcessWithMalformedTemplate() {
        String template = "${hash:test";
        Exchange<?, ?> processedExchange = processor.process(testExchange, template, new HashMap<>());

        assertThat(processedExchange).isInstanceOf(HttpExchange.class);
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
