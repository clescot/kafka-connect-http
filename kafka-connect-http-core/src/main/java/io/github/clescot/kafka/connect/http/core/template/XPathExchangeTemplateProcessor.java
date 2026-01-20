package io.github.clescot.kafka.connect.http.core.template;

import io.github.clescot.kafka.connect.http.core.*;
import io.github.clescot.kafka.connect.sse.core.SseExchange;
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
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * XPath template processor for Exchange.
 * Allows extracting and transforming data from any Exchange implementation using XPath expressions.
 */
public class XPathExchangeTemplateProcessor implements ExchangeTemplateProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(XPathExchangeTemplateProcessor.class);

    public static final String NAME = "xpath";
    private static final Pattern XPATH_PATTERN = Pattern.compile("\\$\\{xpath:(.*?)\\}");
    private static final XPathFactory XPATH_FACTORY = XPathFactory.newInstance();

    private static final int MAX_XPATH_LENGTH = 500;
    private static final int MAX_XML_CONTENT_LENGTH = 500000;
    private static final long MAX_XPATH_TIMEOUT_MS = 3000;

    @Override
    public <R extends Request, S extends Response, E extends Exchange<R, S>> E process(
            @NotNull E exchange, @NotNull String template, Map<String, Object> context) {
        LOGGER.debug("Processing template with XPath: {}", template);

        Matcher matcher = XPATH_PATTERN.matcher(template);
        if (!matcher.find()) {
            return exchange;
        }

        String xpathExpression = matcher.group(1);
        LOGGER.debug("Found XPath expression: {}", xpathExpression);

        try {
            Object result = evaluateXPath(exchange, xpathExpression);

            if (result != null) {
                String resultValue = result.toString();
                LOGGER.debug("XPath result for '{}': {}", xpathExpression, resultValue);
                return (E) setContent(exchange, resultValue);
            } else {
                LOGGER.debug("XPath expression '{}' returned null, returning empty content", xpathExpression);
                return (E) setContent(exchange, "");
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to evaluate XPath expression '{}': {}", xpathExpression, e.getMessage());
            LOGGER.debug("Exception details:", e);
        }

        return (E) setContent(exchange, "");
    }

    private Object evaluateXPath(Exchange<?, ?> exchange, String xpathExpression) {
        try {
            if (xpathExpression.length() > MAX_XPATH_LENGTH) {
                LOGGER.warn("XPath expression too long ({} characters), max allowed is {}: {}",
                        xpathExpression.length(), MAX_XPATH_LENGTH, xpathExpression.substring(0, 100) + "...");
                return null;
            }

            XPath xpath = XPATH_FACTORY.newXPath();

            long startTime = System.currentTimeMillis();
            XPathExpression expr = xpath.compile(xpathExpression);
            long compilationTime = System.currentTimeMillis() - startTime;

            if (compilationTime > MAX_XPATH_TIMEOUT_MS) {
                LOGGER.warn("XPath expression compilation took too long ({}ms), possible malicious pattern: {}",
                        compilationTime, xpathExpression);
                return null;
            }

            String content = exchange.getContent();
            if (content != null && isXmlContent(content)) {

                if (content.length() > MAX_XML_CONTENT_LENGTH) {
                    LOGGER.warn("XML content too large for XPath processing ({} characters), max allowed is {}: {}",
                            content.length(), MAX_XML_CONTENT_LENGTH, content.substring(0, 100) + "...");
                    return null;
                }

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

    private boolean isXmlContent(String content) {
        if (content == null || content.trim().isEmpty()) {
            return false;
        }
        String trimmed = content.trim();
        return trimmed.startsWith("<") || trimmed.startsWith("<?xml");
    }

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

    @SuppressWarnings("unchecked")
    private <R extends Request, S extends Response> Exchange<R, S> setContent(
            Exchange<R, S> exchange, String content) {

        if (exchange instanceof HttpExchange httpExchange) {
            HttpRequest request = httpExchange.getRequest();
            HttpResponse originalResponse = httpExchange.getResponse();

            HttpResponse newResponse;
            if (originalResponse != null) {
                newResponse = (HttpResponse) originalResponse.clone();
                newResponse.setBodyAsString(content);
            } else {
                newResponse = new HttpResponse(200, "OK");
                newResponse.setBodyAsString(content);
            }

            return (Exchange<R, S>) HttpExchange.Builder.anHttpExchange()
                    .withHttpRequest(request)
                    .withHttpResponse(newResponse)
                    .withDuration(httpExchange.getDurationInMillis())
                    .at(httpExchange.getMoment())
                    .withAttempts(httpExchange.getAttempts())
                    .withAttributes(new HashMap<>(httpExchange.getAttributes()))
                    .withTimings(new HashMap<>(httpExchange.getTimings()))
                    .build();
        }

        if (exchange instanceof SseExchange) {
            SseExchange sseExchange = (SseExchange) exchange;
            return (Exchange<R, S>) sseExchange.setContent(content);
        }

        LOGGER.warn("Unsupported exchange type: {}. Cannot set content.", exchange.getClass().getName());
        return exchange;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean supports(String template) {
        return template != null && XPATH_PATTERN.matcher(template).find();
    }

    @Override
    public String getTemplatePattern() {
        return "xpath:[^}]+";
    }
}
