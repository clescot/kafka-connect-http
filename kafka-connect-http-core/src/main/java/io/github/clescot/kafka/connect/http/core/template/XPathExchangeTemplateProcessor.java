package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.Exchange;
import io.github.clescot.kafka.connect.http.core.Request;
import io.github.clescot.kafka.connect.http.core.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * XPath template processor for Exchange.
 * Allows extracting and transforming data from any Exchange implementation using XPath expressions.
 * This processor works with XML content in requests or responses.
 */
public class XPathExchangeTemplateProcessor implements ExchangeTemplateProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(XPathExchangeTemplateProcessor.class);
    
    public static final String NAME = "xpath";
    private static final Pattern XPATH_PATTERN = Pattern.compile("\\$\\{xpath:(.*?)\\}");
    private static final XPathFactory XPATH_FACTORY = XPathFactory.newInstance();

    @Override
    public <R extends Request,S extends Response,E extends Exchange<R,S>> Exchange<R, S> process(@NotNull E exchange, @NotNull String template, Map<String, Object> context) {
        LOGGER.debug("Processing template with XPath: {}", template);
        
        // Start with the original exchange
        Exchange<R, S> modifiedExchange = exchange;
        
        // Process the template to extract XPath expressions
        Matcher matcher = XPATH_PATTERN.matcher(template);
        
        LOGGER.debug("Looking for XPath patterns in template: {}", template);
        boolean foundAny = false;
        
        while (matcher.find()) {
            foundAny = true;
            String xpathExpression = matcher.group(1);
            LOGGER.debug("Found XPath expression: {}", xpathExpression);
            
            try {
                // Try to evaluate the XPath expression against the exchange
                Object result = evaluateXPath(exchange, xpathExpression);
                
                if (result != null) {
                    LOGGER.debug("XPath result for '{}': {}", xpathExpression, result);
                    
                    // Add the result to attributes using the exchange's withAttribute method
                    String attributeName = "xpath_" + xpathExpression.replaceAll("[^a-zA-Z0-9_]", "_");
                    modifiedExchange = modifiedExchange.withAttribute(attributeName, result.toString());
                    LOGGER.debug("Added attribute: {} = {}", attributeName, result.toString());
                } else {
                    LOGGER.debug("XPath expression '{}' returned null", xpathExpression);
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to evaluate XPath expression '{}': {}", xpathExpression, e.getMessage());
                LOGGER.debug("Exception details:", e);
            }
        }
        
        if (!foundAny) {
            LOGGER.debug("No XPath expressions found in template");
        }
        
        return modifiedExchange;
    }

    /**
     * Evaluate an XPath expression against the Exchange.
     * 
     * @param exchange the exchange to evaluate against
     * @param xpathExpression the XPath expression
     * @return the result of the evaluation, or null if failed
     */
    private Object evaluateXPath(Exchange<?, ?> exchange, String xpathExpression) {
        try {
            XPath xpath = XPATH_FACTORY.newXPath();
            XPathExpression expr = xpath.compile(xpathExpression);
            
            // Try to evaluate against content if it's XML
            String content = exchange.getContentAsString();
            if (content != null && isXmlContent(content)) {
                Document doc = parseXml(content);
                if (doc != null) {
                    return expr.evaluate(doc, XPathConstants.STRING);
                }
            }
            
            LOGGER.debug("No XML content found in exchange for XPath evaluation");
            return null;
            
        } catch (Exception e) {
            LOGGER.debug("XPath evaluation failed for '{}': {}", xpathExpression, e.getMessage());
            return null;
        }
    }

    /**
     * Check if content appears to be XML.
     */
    private boolean isXmlContent(String content) {
        if (content == null || content.trim().isEmpty()) {
            return false;
        }
        String trimmed = content.trim();
        return trimmed.startsWith("<") || trimmed.startsWith("<?xml");
    }

    /**
     * Parse XML string into a Document.
     */
    private Document parseXml(String xmlContent) {
        try {
            javax.xml.parsers.DocumentBuilderFactory factory = javax.xml.parsers.DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            javax.xml.parsers.DocumentBuilder builder = factory.newDocumentBuilder();
            return builder.parse(new InputSource(new StringReader(xmlContent)));
        } catch (Exception e) {
            LOGGER.debug("Failed to parse XML content: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean supports(String template) {
        return template != null && XPATH_PATTERN.matcher(template).find();
    }
}