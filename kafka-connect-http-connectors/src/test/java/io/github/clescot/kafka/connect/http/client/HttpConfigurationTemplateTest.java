package io.github.clescot.kafka.connect.http.client;

import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClient;
import io.github.clescot.kafka.connect.http.client.okhttp.OkHttpClientFactory;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.github.clescot.kafka.connect.http.core.HttpRequest;
import io.github.clescot.kafka.connect.http.core.HttpResponse;
import io.github.clescot.kafka.connect.http.core.template.ExchangeTemplateManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HttpConfigurationTemplateTest {

    @Mock
    private OkHttpClient mockClient;

    @Mock
    private ExecutorService mockExecutorService;

    private HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> httpConfiguration;
    private Map<String, String> settings;

    @BeforeEach
    void setUp() {
        settings = new HashMap<>();
        settings.put("exchange.template", "${jsonpath:$.response.statusCode} ${random.int:1:100}");
        
        httpConfiguration = new HttpConfiguration<>(
                "test-config",
                mockClient,
                mockExecutorService,
                null, // no retry policy for this test
                settings
        );
    }

    @Test
    void testTemplateManagerInitialization() {
        assertThat(httpConfiguration).isNotNull();
        assertThat(httpConfiguration.getExchangeTemplate()).isEqualTo("${jsonpath:$.response.statusCode} ${random.int:1:100}");
        
        ExchangeTemplateManager templateManager = httpConfiguration.getTemplateManager();
        assertThat(templateManager).isNotNull();
        assertThat(templateManager.getProcessors()).isNotEmpty();
        assertThat(templateManager.canProcess("${jsonpath:$.test}")).isTrue();
        assertThat(templateManager.canProcess("${random.int}")).isTrue();
    }

    @Test
    void testEnrichExchangeWithTemplate() {
        // Create a test exchange
        HttpRequest request = new HttpRequest("http://example.com/test", HttpRequest.Method.GET);
        HttpResponse response = new HttpResponse(200, "OK");
        response.setBodyAsString("{\"status\": \"success\", \"code\": 200}");
        
        HttpExchange originalExchange = new HttpExchange(
                request,
                response,
                100L,
                OffsetDateTime.now(),
                new AtomicInteger(1),
                true
        );

        // Call enrichExchange (this is what gets called after the HTTP call)
        HttpExchange enrichedExchange = httpConfiguration.enrichExchange(originalExchange);

        // Verify that template processing was applied
        assertThat(enrichedExchange).isNotNull();
        assertThat(enrichedExchange.getAttributes()).isNotEmpty();
        
        // Should have JSONPath attributes
        boolean hasJsonPathAttribute = enrichedExchange.getAttributes().keySet().stream()
                .anyMatch(key -> key.startsWith("jsonpath_"));
        assertThat(hasJsonPathAttribute).isTrue();
        
        // Should have random attributes
        boolean hasRandomAttribute = enrichedExchange.getAttributes().keySet().stream()
                .anyMatch(key -> key.startsWith("random_"));
        assertThat(hasRandomAttribute).isTrue();
    }

    @Test
    void testEnrichExchangeWithoutTemplate() {
        // Create configuration without template
        Map<String, String> noTemplateSettings = new HashMap<>();
        HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> noTemplateConfig = 
                new HttpConfiguration<>("no-template-config", mockClient, mockExecutorService, null, noTemplateSettings);

        HttpRequest request = new HttpRequest("http://example.com/test", HttpRequest.Method.GET);
        HttpResponse response = new HttpResponse(200, "OK");
        
        HttpExchange originalExchange = new HttpExchange(
                request,
                response,
                100L,
                OffsetDateTime.now(),
                new AtomicInteger(1),
                true
        );

        // Should return the original exchange unchanged
        HttpExchange enrichedExchange = noTemplateConfig.enrichExchange(originalExchange);
        assertThat(enrichedExchange).isEqualTo(originalExchange);
    }

    @Test
    void testCustomProcessorConfiguration() {
        // Test with custom processor configuration
        Map<String, String> customSettings = new HashMap<>();
        customSettings.put("exchange.template", "${jsonpath:$.response.body}");
        customSettings.put("exchange.template.processors", "jsonpath,random");
        
        HttpConfiguration<OkHttpClient, okhttp3.Request, okhttp3.Response> customConfig = 
                new HttpConfiguration<>("custom-config", mockClient, mockExecutorService, null, customSettings);

        ExchangeTemplateManager templateManager = customConfig.getTemplateManager();
        assertThat(templateManager).isNotNull();
        assertThat(templateManager.getProcessors()).hasSizeGreaterThanOrEqualTo(2);
    }
}