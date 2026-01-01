# Exchange Template Customization Examples

This document provides examples of how to customize Exchange output using the template system in the kafka-connect-http project.

## Overview

The template system allows you to customize Exchange output (HttpExchange, SseExchange, etc.) by:
1. Extracting data using JSONPath expressions
2. Extracting data using XPath expressions (for XML content)
3. Extracting data using JMESPath expressions
4. Extracting data using Regex patterns
5. Generating random values
6. Working with dates and times
7. Applying conditional logic
8. Creating hashes and digests
9. Performing mathematical operations
10. Creating custom processors

The template system works with any Exchange implementation, including both HTTP and SSE configurations.

## Basic Configuration

To enable template processing, add the following to your connector configuration:

```properties
# Enable template processing
exchange.template=your_template_here

# Optionally specify which processors to use (default includes all built-in processors)
exchange.template.processors=jsonpath,random,xpath,jmespath,regex,headerparam,datetime,conditional,hash,math
```

The template system works with both HTTP and SSE configurations:

```properties
# For HTTP connectors
exchange.template=${jsonpath:$.request.url} ${random.uuid}

# For SSE connectors
exchange.template=${jsonpath:$.event.data} ${datetime:now:yyyy-MM-dd HH:mm:ss}
```

## Connector-Specific Usage

### HTTP Connector Configuration

The HTTP connector supports template processing for both source and sink connectors.

#### HTTP Sink Connector Example

```json
{
   "name" : "http-sink-connector",
   "tasks.max" : "1",
   "connector.class" : "io.github.clescot.kafka.connect.http.sink.HttpSinkConnector",
   "http.request.url" : "http://api.example.com/webhook",
   "http.request.method" : "POST",
   "exchange.template" : "${jsonpath:$.request.body.user.id} ${jsonpath:$.request.body.timestamp}",
   "exchange.template.processors" : "jsonpath,datetime",
   "topics" : "user-events"
}
```

#### HTTP Source Connector Example

```json
{
   "tasks.max" : "1",
   "connector.class" : "io.github.clescot.kafka.connect.http.source.queue.HttpInMemoryQueueSourceConnector",
   "exchange.template" : "${jsonpath:$.request.headers.X-Request-ID} ${datetime:now:yyyy-MM-dd HH:mm:ss}",
   "exchange.template.processors" : "jsonpath,datetime",
   "queue.name" : "in-memory-queue",
   "topic" : "incoming-requests"
}
```

### SSE Connector Configuration

The SSE (Server-Sent Events) connector supports template processing for incoming events.

#### Basic SSE Source Connector Example

```json
{
   "name" : "sse-source-connector",
   "tasks.max" : "1",
   "connector.class" : "io.github.clescot.kafka.connect.sse.client.okhttp.SseSourceConnector",
   "config.ids" : "sse-config",
   "config.sse-config.url" : "http://stream.example.com/events",
   "config.sse-config.topic" : "streaming-events",
   "exchange.template" : "${jsonpath:$.content}",
   "exchange.template.processors" : "jsonpath"
}
```

#### Advanced SSE Configuration with Multiple Processors

```json
{
   "name" : "advanced-sse-connector",
   "tasks.max" : "1",
   "connector.class" : "io.github.clescot.kafka.connect.sse.client.okhttp.SseSourceConnector",
   "config.ids" : "config1,config2",
   "config.config1.url" : "http://api.example.com/user-events",
   "config.config1.topic" : "user-events",
   "config.config1.exchange.template" : "${jsonpath:$.event.data.user.id} ${jsonpath:$.event.data.user.name}",
   "config.config1.exchange.template.processors" : "jsonpath",
   "config.config2.url" : "http://api.example.com/system-metrics",
   "config.config2.topic" : "system-metrics",
   "config.config2.exchange.template" : "${jsonpath:$.event.data.cpu} ${jsonpath:$.event.data.memory} ${datetime:now:yyyy-MM-dd HH:mm:ss}",
   "config.config2.exchange.template.processors" : "jsonpath,datetime"
}
```

## Understanding Exchange Structure

### HTTP Exchange Structure

When using template processing with HTTP connectors, the exchange structure includes:

```json
{
  "request": {
    "url": "http://example.com/api",
    "method": "POST",
    "headers": {
      "Content-Type": "application/json",
      "Authorization": "Bearer token"
    },
    "body": "{\"user\": \"john\", \"action\": \"login\"}"
  },
  "response": {
    "statusCode": 200,
    "headers": {
      "Content-Type": "application/json"
    },
    "body": "{\"success\": true, \"userId\": 123}"
  },
  "content": "{\"success\": true, \"userId\": 123}",
  "attributes": {},
  "metadata": {
    "requestUrl": "http://example.com/api",
    "requestMethod": "POST",
    "responseStatus": 200
  }
}
```

### SSE Exchange Structure

When using template processing with SSE connectors, the exchange structure includes:

```json
{
  "request": {
    "url": "http://stream.example.com/events",
    "method": "GET"
  },
  "response": {
    "id": "event-123",
    "type": "user-action",
    "data": "{\"userId\": 456, \"action\": \"click\", \"timestamp\": 1234567890}",
    "attributes": {},
    "success": true
  },
  "content": "{\"userId\": 456, \"action\": \"click\", \"timestamp\": 1234567890}",
  "attributes": {},
  "metadata": {
    "eventId": "event-123",
    "eventType": "user-action",
    "requestUrl": "http://stream.example.com/events",
    "requestMethod": "GET"
  }
}
```

## Common Template Patterns

### Extracting Data from HTTP Requests

```properties
# Extract request headers and body
exchange.template=${jsonpath:$.request.headers.X-API-Key} ${jsonpath:$.request.body.user.id}
```

### Iterating Over Arrays

The template system supports extracting and processing array data from exchanges.

#### Extract All Array Elements

```properties
# Extract all items from an array in HTTP response
exchange.template=${jsonpath:$.response.body.items[*].id}
```

This extracts all IDs from the items array and adds them as separate attributes.

#### Extract Specific Array Elements

```properties
# Extract first and last elements from array
exchange.template=${jsonpath:$.response.body.users[0].name} ${jsonpath:$.response.body.users[-1].name}
```

#### Extract Array Length

```properties
# Extract the length of an array
exchange.template=${jsonpath:$.response.body.items.length()}
```

#### Extract Filtered Array Elements

```properties
# Extract only active users from array
exchange.template=${jsonpath:$.response.body.users[?(@.active==true)].name}
```

#### Extract Array Slices

```properties
# Extract first 5 elements from array
exchange.template=${jsonpath:$.response.body.logs[0:5].message}
```

### Processing Array Data in SSE Events

```properties
# Extract all event types from SSE event array
exchange.template=${jsonpath:$.event.data.events[*].type}

# Extract specific user IDs from nested array
exchange.template=${jsonpath:$.event.data.user.orders[*].id}
```

### Advanced Array Processing with JMESPath

```properties
# Use JMESPath to transform array data
exchange.template=${jmespath:response.body.items | [].{id: id, name: name}}

# Filter and project array data
exchange.template=${jmespath:response.body.users | [?age > 25].name}

# Calculate statistics from array
exchange.template=${jmespath:response.body.orders | length(@)} ${jmespath:response.body.orders | [].price | sum(@)}
```

## Array Processing vs For Loops

### Understanding the Approach

The template system uses **declarative array processing** rather than traditional imperative `for` loops. This means you specify *what* data you want to extract, not *how* to iterate through it.

#### What You Can Do ✅

```properties
# Extract all elements from array (declarative)
exchange.template=${jsonpath:$.response.body.items[*].id}

# Extract with filtering (declarative with condition)
exchange.template=${jsonpath:$.response.body.items[?(@.price > 50)].name}

# Extract with transformation (declarative mapping)
exchange.template=${jmespath:response.body.items | [].{id: id, displayName: name}}
```

#### What You Can't Do ❌

```properties
# Traditional for loop syntax is not supported
exchange.template=${for:item in $.response.body.items}${jsonpath:item.id}${endfor}

# While loops are not supported
exchange.template=${while:condition}${jsonpath:$.data}${endwhile}
```

### Why Declarative Processing?

1. **Safety**: Prevents infinite loops and complex control flow in templates
2. **Performance**: Optimized for bulk operations rather than element-by-element processing
3. **Simplicity**: Focuses on what data you need, not how to get it
4. **Consistency**: Works the same way across all processors and exchange types

### When You Need Loop-Like Behavior

#### 1. Extract All Array Elements

```properties
# This is the closest to a "for each" loop
exchange.template=${jsonpath:$.response.body.users[*].id}
```

Given this data:
```json
{
  "users": [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"},
    {"id": 3, "name": "Charlie"}
  ]
}
```

This creates attributes:
- `jsonpath_response_body_users_0_id`: `1`
- `jsonpath_response_body_users_1_id`: `2`
- `jsonpath_response_body_users_2_id`: `3`

#### 2. Extract with Index-Based Access

```properties
# Extract specific indices (like array[index] in a loop)
exchange.template=${jsonpath:$.response.body.items[0].id} ${jsonpath:$.response.body.items[1].id}
```

#### 3. Extract with Range (Slice)

```properties
# Extract a range of elements (like a bounded loop)
exchange.template=${jsonpath:$.response.body.logs[0:10].message}
```

#### 4. Extract with Filtering (Conditional Loop)

```properties
# Extract elements matching conditions (like loop with if)
exchange.template=${jsonpath:$.response.body.products[?(@.active==true)].name}
```

#### 5. Complex Transformations with JMESPath

```properties
# Transform array data (like loop with mapping function)
exchange.template=${jmespath:response.body.items | [].{id: id, displayName: name, formattedPrice: \"$\" & price}}
```

### Advanced Array Processing Patterns

#### Pattern 1: Extract Multiple Fields from Array

```properties
# Extract both ID and name from all users
exchange.template=${jsonpath:$.response.body.users[*].id} ${jsonpath:$.response.body.users[*].name}
```

#### Pattern 2: Nested Array Processing

```properties
# Process nested arrays
exchange.template=${jsonpath:$.response.body.orders[*].items[*].productId}
```

#### Pattern 3: Array Length Calculation

```properties
# Get array length (like counting loop iterations)
exchange.template=${jsonpath:$.response.body.items.length()}
```

#### Pattern 4: Combine Array Processing with Other Data

```properties
# Mix array data with scalar data
exchange.template=${jsonpath:$.response.body.user.id} ${jsonpath:$.response.body.orders[*].status}
```

### Performance Considerations

#### 1. Limit Array Size

```properties
# Process only what you need
exchange.template=${jsonpath:$.response.body.largeArray[0:100].id}
```

#### 2. Use Specific Paths

```properties
# Be precise to avoid unnecessary processing
exchange.template=${jsonpath:$.response.body.data.items[*].productId}
```

#### 3. Filter Early

```properties
# Filter before extracting to reduce processing
exchange.template=${jsonpath:$.response.body.items[?(@.active==true)][0:50].id}
```

## Array Processing Examples

### HTTP Response with Array Data

```json
{
   "name" : "http-array-connector",
   "tasks.max" : "1",
   "connector.class" : "io.github.clescot.kafka.connect.http.sink.HttpSinkConnector",
   "exchange.template" : "${jsonpath:$.response.body.products[*].id} ${jsonpath:$.response.body.products[*].name}",
   "exchange.template.processors" : "jsonpath",
   "topics" : "product-events"
}
```

Given this HTTP response:
```json
{
  "products": [
    {"id": 1, "name": "Product A", "price": 10.99},
    {"id": 2, "name": "Product B", "price": 20.50},
    {"id": 3, "name": "Product C", "price": 15.75}
  ]
}
```

This template will extract all product IDs and names, creating attributes like:
- `jsonpath_response_body_products_0_id`: `1`
- `jsonpath_response_body_products_0_name`: `"Product A"`
- `jsonpath_response_body_products_1_id`: `2`
- `jsonpath_response_body_products_1_name`: `"Product B"`
- etc.

### SSE Event with Array Data

```json
{
   "name" : "sse-array-connector",
   "tasks.max" : "1",
   "connector.class" : "io.github.clescot.kafka.connect.sse.client.okhttp.SseSourceConnector",
   "config.ids" : "array-config",
   "config.array-config.url" : "http://stream.example.com/messages",
   "config.array-config.topic" : "message-events",
   "exchange.template" : "${jsonpath:$.event.data.messages[*].content} ${jsonpath:$.event.data.messages[*].timestamp}",
   "exchange.template.processors" : "jsonpath"
}
```

Given this SSE event data:
```json
{
  "messages": [
    {"content": "Hello", "timestamp": 1234567890, "user": "alice"},
    {"content": "World", "timestamp": 1234567891, "user": "bob"}
  ]
}
```

This template will extract all message contents and timestamps.

### Combining Array Processing with Other Processors

```json
{
   "name" : "combined-processor-connector",
   "tasks.max" : "1",
   "connector.class" : "io.github.clescot.kafka.connect.sse.client.okhttp.SseSourceConnector",
   "config.ids" : "combined-config",
   "config.combined-config.url" : "http://stream.example.com/items",
   "config.combined-config.topic" : "item-events",
   "exchange.template" : "${jsonpath:$.event.data.items[*].id} ${datetime:now:yyyy-MM-dd HH:mm:ss}",
   "exchange.template.processors" : "jsonpath,datetime"
}
```

### Array Processing with Conditional Logic

```json
{
   "name" : "conditional-array-connector",
   "tasks.max" : "1",
   "connector.class" : "io.github.clescot.kafka.connect.http.sink.HttpSinkConnector",
   "exchange.template" : "${jsonpath:$.response.body.items[?(@.price > 50)].id}",
   "exchange.template.processors" : "jsonpath",
   "topics" : "high-value-items"
}
```

## Array Processing Best Practices

### 1. Limit Array Size for Performance

```properties
# Process only first 100 elements to avoid performance issues
exchange.template=${jsonpath:$.response.body.largeArray[0:100].id}
```

### 2. Use Specific Paths for Clarity

```properties
# Be specific about array paths
exchange.template=${jsonpath:$.response.body.data.items[*].productId}
```

### 3. Handle Empty Arrays Gracefully

The template system handles empty arrays gracefully - no attributes are created if the array is empty.

### 4. Combine with Other Processors

```properties
# Extract array data and add metadata
exchange.template=${jsonpath:$.event.data.items[*].id} ${random.uuid} ${datetime:now}
exchange.template.processors=jsonpath,random,datetime
```

### 5. Use JMESPath for Complex Transformations

```properties
# Use JMESPath for advanced array operations
exchange.template=${jmespath:response.body.items | [].{id: id, category: category}}
```

## Common Template Patterns

### Extracting Data from HTTP Requests

```json
{
   "exchange.template" : "${jsonpath:$.request.headers.X-API-Key} ${jsonpath:$.request.body.user.id}",
   "exchange.template.processors" : "jsonpath"
}
```

### Extracting Data from HTTP Responses

```json
{
   "exchange.template" : "${jsonpath:$.response.statusCode} ${jsonpath:$.response.body.success}",
   "exchange.template.processors" : "jsonpath"
}
```

### Extracting Data from SSE Events

```json
{
   "exchange.template" : "${jsonpath:$.event.id} ${jsonpath:$.event.data}",
   "exchange.template.processors" : "jsonpath"
}
```

### Extracting Nested JSON Data

```json
{
   "exchange.template" : "${jsonpath:$.event.data.user.profile.name} ${jsonpath:$.event.data.timestamp}",
   "exchange.template.processors" : "jsonpath"
}
```

### Adding Timestamps

```json
{
   "exchange.template" : "${datetime:now:yyyy-MM-dd HH:mm:ss}",
   "exchange.template.processors" : "datetime"
}
```

### Combining Multiple Processors

```json
{
   "exchange.template" : "${jsonpath:$.event.data.user.id} ${datetime:now:yyyy-MM-dd} ${random.uuid}",
   "exchange.template.processors" : "jsonpath,random,datetime"
}
```

## Best Practices

### 1. Start with Simple Templates

Begin with simple templates and gradually add complexity:

```json
{
   "exchange.template" : "${jsonpath:$.event.data}",
   "exchange.template.processors" : "jsonpath"
}
```

Then add more extractions:

```json
{
   "exchange.template" : "${jsonpath:$.event.data} ${jsonpath:$.event.id}",
   "exchange.template.processors" : "jsonpath"
}
```

Finally add metadata:

```json
{
   "exchange.template" : "${jsonpath:$.event.data} ${datetime:now:yyyy-MM-dd HH:mm:ss}",
   "exchange.template.processors" : "jsonpath,datetime"
}
```

### 2. Use Specific Processor Lists

Only enable the processors you need to improve performance:

```json
{
   "exchange.template.processors" : "jsonpath"
}
```

For JSON + timestamp:

```json
{
   "exchange.template.processors" : "jsonpath,datetime"
}
```

### 3. Handle Errors Gracefully

Template processing errors are logged but don't stop event processing. The original event is preserved.

### 4. Test Templates

Test your templates with sample data before deploying to production.

### 5. Monitor Performance

Complex templates with many processors can impact performance. Monitor and optimize as needed.

## Troubleshooting

### Template Not Working?

1. **Check Syntax**: Ensure you're using the correct syntax `${processor:expression}`
2. **Verify Processor**: Make sure the processor is enabled in `exchange.template.processors`
3. **Check Logs**: Look for warnings about unsupported templates or processor errors
4. **Test Data**: Verify your template works with the actual data structure

### Common Issues

- **No processor found**: The template syntax is incorrect or the processor isn't enabled
- **JSONPath not found**: The path doesn't exist in your data structure
- **Null values**: The template processor returns null for missing data (this is normal)

## Advanced Usage

### Custom Processors

You can create custom template processors by implementing the `ExchangeTemplateProcessor` interface and registering them via Java's Service Loader mechanism.

### Conditional Processing

Use conditional processors to add logic to your templates:

```properties
exchange.template=${conditional:response.statusCode >= 200 && response.statusCode < 300:SUCCESS:FAILURE}
exchange.template.processors=jsonpath,conditional
```

### Data Transformation

Use JMESPath for complex data transformations:

```properties
exchange.template=${jmespath:event.data | {userId: user.id, timestamp: metadata.timestamp}}
exchange.template.processors=jmespath
```

## JSONPath Reference

### Basic Syntax

- `.child` - Child node
- `['child']` - Child node with special characters
- `[index]` - Array index
- `[start:end]` - Array slice
- `*` - Wildcard (all elements)
- `..` - Recursive descent

### Examples

- `$.store.book[0].title` - First book title
- `$.store.book[*].author` - All authors
- `$.store..price` - All prices (recursive)
- `$..book[?(@.price < 10)]` - Books cheaper than $10

### Array-Specific Examples

- `$.users[*].name` - All user names (iterates over array)
- `$.orders[0:5].id` - First 5 order IDs (array slice)
- `$.products[?(@.price > 50)]` - Products over $50 (filter)
- `$.items.length()` - Number of items in array
- `$.data.events[*].{id: id, type: type}` - Project specific fields from array elements

### Array Processing in Exchange Context

For HTTP exchanges:
- `$.response.body.items[*].id` - All item IDs from response
- `$.request.body.users[*].email` - All user emails from request

For SSE exchanges:
- `$.event.data.messages[*].content` - All message contents
- `$.event.data.orders[*].status` - All order statuses

## JMESPath Reference

### Basic Syntax

- `field` - Field access
- `list[index]` - List index
- `*` - Wildcard projection
- `|` - Pipe operator for transformations
- `&&`, `||`, `!` - Logical operators
- `==`, `!=`, `<`, `>`, `<=`, `>=` - Comparison operators

### Examples

- `people[?age > `30`]` - People over 30
- `orders[].items[].price | sum(@)` - Sum of all order item prices
- `{name: name, age: age}` - Project specific fields

## Conclusion

The template system provides powerful data extraction and transformation capabilities for both HTTP and SSE connectors. By understanding the exchange structure and using the appropriate processors, you can customize how data flows through your Kafka Connect pipelines.

For more information, refer to the individual processor documentation and the main project documentation.

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

## JMESPath Examples

JMESPath provides a more powerful way to extract and transform JSON data.

### Extract and Transform JSON Data

```properties
exchange.template=${jmespath:request.body | {user: name, id: userId}}
```

This extracts and transforms the request body JSON.

### Filter and Project Data

```properties
exchange.template=${jmespath:response.body.orders[?status=='completed']}
```

This filters the response body to only include completed orders.

## Regex Examples

### Extract Data Using Regular Expressions

```properties
exchange.template=${regex:request.body:/user=([^&]+)/:1}
```

This extracts the first capture group from a regex match on the request body.

### Extract Multiple Groups

```properties
exchange.template=${regex:request.url:/api/([^/]+)/([^/]+)/:1} ${regex:request.url:/api/([^/]+)/([^/]+)/:2}
```

This extracts multiple capture groups from the URL path.

## DateTime Examples

### Add Current Timestamp

```properties
exchange.template=${datetime:now:yyyy-MM-dd HH:mm:ss}
```

This adds the current timestamp to the exchange.

### Format Specific Date

```properties
exchange.template=${datetime:2023-01-01:yyyy-MM-dd}
```

This formats a specific date.

### Add Timestamp with Custom Format

```properties
exchange.template=ProcessedAt: ${datetime:now:yyyyMMdd-HHmmssSSS}
```

This adds a timestamp with a custom format.

## Conditional Examples

### Conditional Processing

```properties
exchange.template=${conditional:response.statusCode >= 200 && response.statusCode < 300:SUCCESS:FAILURE}
```

This adds a SUCCESS or FAILURE attribute based on the status code.

### Complex Conditions

```properties
exchange.template=${conditional:request.method == 'GET' && response.statusCode == 200:READ_SUCCESS:OTHER}
```

This evaluates complex conditions.

## Hash Examples

### Create Hash of Request Body

```properties
exchange.template=RequestHash: ${hash:request.body:SHA-256}
```

This creates a SHA-256 hash of the request body.

### Create MD5 Hash

```properties
exchange.template=ContentHash: ${hash:response.body:MD5}
```

This creates an MD5 hash of the response body.

## Math Examples

### Perform Mathematical Operations

```properties
exchange.template=Total: ${math:request.body.price * request.body.quantity}
```

This calculates the total price.

### Complex Expressions

```properties
exchange.template=DiscountedPrice: ${math:request.body.price * (1 - request.body.discount)}
```

This calculates a discounted price.

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

### JMESPath Syntax

```
${jmespath:expression}
```

Where `expression` is a valid JMESPath expression for querying and transforming JSON data.

### XPath Syntax

```
${xpath:expression}
```

Where `expression` is a valid XPath expression that will be evaluated against XML content in requests or responses.

### Regex Syntax

```
${regex:input:pattern:group}
```

Where:
- `input` is the text to match against
- `pattern` is the regular expression pattern
- `group` is the capture group number (1, 2, 3, etc.)

### DateTime Syntax

```
${datetime:input:format}
```

Where:
- `input` can be `now` for current time or a specific date/time string
- `format` is the output format using Java DateTimeFormatter patterns

### Conditional Syntax

```
${conditional:condition:trueValue:falseValue}
```

Where:
- `condition` is a boolean expression
- `trueValue` is returned if condition is true
- `falseValue` is returned if condition is false

### Hash Syntax

```
${hash:input:algorithm}
```

Where:
- `input` is the text to hash
- `algorithm` is the hash algorithm (SHA-256, MD5, SHA-1, etc.)

### Math Syntax

```
${math:expression}
```

Where `expression` is a mathematical expression that can reference exchange data.

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