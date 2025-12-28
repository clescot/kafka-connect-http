# Exchange Template Customization Examples

This document provides examples of how to customize Exchange output using the template system in the camille-kafka-connect-http project.

## Overview

The template system allows you to customize HttpExchange output by:
1. Extracting data using JSONPath expressions
2. Extracting data using XPath expressions (for XML content)
3. Generating random values
4. Creating custom processors

## Basic Configuration

To enable template processing, add the following to your connector configuration:

```properties
# Enable template processing
exchange.template=your_template_here

# Optionally specify which processors to use (default includes all built-in processors)
exchange.template.processors=jsonpath,random,xpath
```

## JSONPath Examples

### Extract Request URL and Response Status

```properties
exchange.template=${jsonpath:$.request.url} ${jsonpath:$.response.statusCode}
```

This will:
- Extract the request URL from the exchange
- Extract the response status code
- Add them as attributes to the exchange with names like `jsonpath_request_url` and `jsonpath_response_statusCode`

### Extract Complex Data

```properties
exchange.template=${jsonpath:$.request.headers.Content-Type} ${jsonpath:$.response.body}
```

This extracts:
- The Content-Type header from the request
- The entire response body

### Nested JSONPath Expressions

```properties
exchange.template=${jsonpath:$.request.body.user.name} ${jsonpath:$.response.body.data[0].id}
```

This extracts:
- The user name from a nested JSON request body
- The first item's ID from a response array

## XPath Examples

### Extract Data from XML Request

```properties
exchange.template=${xpath://user/name} ${xpath://order/id}
```

This will:
- Extract the user name from an XML request body
- Extract the order ID from an XML request body
- Add them as attributes with names like `xpath_user_name` and `xpath_order_id`

### Extract Data from XML Response

```properties
exchange.template=${xpath://response/status} ${xpath://response/data/item[1]/value}
```

This extracts:
- The status from an XML response
- The value of the first item in a response

## Random Value Examples

### Generate Random IDs

```properties
exchange.template=RequestID: ${random.uuid} TransactionID: ${random.int:1000:9999}
```

This generates:
- A random UUID
- A random integer between 1000-9999

### Generate Test Data

```properties
exchange.template=TestUser: ${random.string:8} Active: ${random.boolean}
```

This generates:
- A random 8-character string
- A random boolean value

## Combined Examples

### Extract and Enhance Data

```properties
exchange.template=${jsonpath:$.request.url} ${random.uuid} ${jsonpath:$.response.statusCode}
```

This combines:
- Request URL extraction
- Random UUID generation
- Response status code extraction

### XML Processing with Random Enhancement

```properties
exchange.template=${xpath://order/id} ${random.uuid} ${xpath://order/status}
```

This combines:
- Order ID extraction from XML
- Random UUID generation
- Order status extraction from XML

## Advanced Usage

### Custom Processor Registration

You can register custom processors by implementing the `ExchangeTemplateProcessor` interface and registering them:

```java
// Create and register a custom processor
ExchangeTemplateManager manager = new ExchangeTemplateManager();
manager.registerProcessor(new MyCustomProcessor());

// Use it in configuration
Map<String, String> settings = new HashMap<>();
settings.put("exchange.template.processors", "jsonpath,random,xpath,mycustom");
```

### Service Loader Discovery

Custom processors can be automatically discovered using Java's Service Loader mechanism by creating a file:

`META-INF/services/io.github.clescot.kafka.connect.http.core.template.ExchangeTemplateProcessor`

With your processor class name as the content.

## Template Syntax Reference

### JSONPath Syntax

```
${jsonpath:expression}
```

Where `expression` is a valid JSONPath expression that can navigate the exchange structure.

### XPath Syntax

```
${xpath:expression}
```

Where `expression` is a valid XPath expression that will be evaluated against XML content in requests or responses.

### Random Syntax

```
${random.type:min:max}
```

Supported types:
- `int`, `integer`: Random integer (min and max required)
- `long`: Random long (min and max required)
- `double`, `float`: Random floating point number (min and max required)
- `uuid`: Random UUID (no parameters)
- `string`, `str`: Random string (length parameter)
- `boolean`, `bool`: Random boolean (no parameters)

## Debugging

Enable debug logging to see template processing details:

```properties
log4j.logger.io.github.clescot.kafka.connect.http.core.template=DEBUG
```

This will show:
- Template parsing details
- Expression evaluation results
- Attribute additions
- Any processing errors

## Performance Considerations

1. **Template Complexity**: Complex templates with many expressions may impact performance
2. **Processor Order**: Processors are evaluated in registration order
3. **Error Handling**: Failed expressions are logged but don't stop processing
4. **Memory Usage**: Large XML/JSON documents may consume significant memory during parsing