# AGENTS.md - Guidelines for Claude Code and Other Agents

This document provides guidelines for agents working on the kafka-connect-http codebase.

## Build Commands

The project uses Maven as the build tool. All commands should be run from the root directory.

```bash
# Compile all modules
mvn clean compile

# Run all tests (unit tests only, integration tests excluded)
mvn clean test

# Run a single test class
mvn test -Dtest=HttpRequestTest

# Run a single test method
mvn test -Dtest=HttpRequestTest#test_serialization

# Run integration tests (IT* tests)
mvn verify -DskipIT=false

# Run all tests including integration tests
mvn clean verify

# Build JAR without running tests
mvn clean package -DskipTests

# Build with code coverage report
mvn clean test -Pcoverage

# Install to local Maven repository
mvn clean install
```

## Project Structure

- **http-core**: Core library containing HTTP request/response models, template processors, and SSE support
- **http-client**: HTTP client implementations (AHCHttpClient, OkHttpClient)
- **kafka-connect-http-connectors**: Kafka Connect sink and source connector implementations
- **report-aggregate**: Aggregated coverage reports

## Code Style Guidelines

### Java Version
- Use Java 17 features (text blocks, pattern matching, records where appropriate)

### Imports
- Use Guava utility classes: `Maps.newHashMap()`, `Lists.newArrayList()`, `Preconditions.checkNotNull()`
- Group imports in this order: io.github.clescot, org.apache.kafka, com.fasterxml.jackson, com.google.guava, org.slf4j, other third-party, java.*
- Use static imports for constants and test assertions

### Naming Conventions
- **Classes**: PascalCase (e.g., `HttpRequest`, `ExchangeTemplateManager`)
- **Methods**: camelCase with underscores for tests (e.g., `test_serialization_with_attributes`)
- **Constants**: UPPER_SNAKE_CASE (e.g., `URL_FIELD`, `DEFAULT_TIMEOUT`)
- **Variables**: camelCase (e.g., `httpRequest`, `headersMap`)
- **Packages**: lowercase (e.g., `io.github.clescot.kafka.connect.http.core`)

### Field Declarations
- Use `@Serial` annotation for Serializable class serialVersionUID fields
- Use `Maps.newHashMap()` and `Lists.newArrayList()` instead of `new HashMap<>()`

### Logging
- Use SLF4J: `private static final Logger LOGGER = LoggerFactory.getLogger(ClassName.class);`
- Use parameterized logging: `LOGGER.debug("message {}", variable)`

### Error Handling
- Use `Preconditions.checkNotNull()` for required argument validation
- Throw `IllegalArgumentException` for invalid inputs
- Use try-with-resources for closeable resources

### Serialization
- Use Jackson annotations: `@JsonProperty`, `@JsonInclude(Include.NON_EMPTY)`, `@JsonIgnore`
- Implement `Serializable` with `@Serial` and `serialVersionUID`
- Provide `toStruct()` method for Kafka Connect Struct conversion

### Testing
- Use JUnit 6 (`org.junit.jupiter.api.*`)
- Use AssertJ for assertions: `assertThat(...)`
- Use `@Nested` for organizing related tests
- Use WireMock for HTTP mocking, Testcontainers for integration testing

### Code Patterns

#### Constructor Validation
```java
public ClassName(String requiredParam, Type optionalParam) {
    Preconditions.checkNotNull(requiredParam, "requiredParam is required");
    this.field = requiredParam;
    this.optionalField = MoreObjects.firstNonNull(optionalParam, defaultValue);
}
```

#### Kafka Connect Schema
```java
public static final Schema SCHEMA = SchemaBuilder
        .struct()
        .name(ClassName.class.getName())
        .version(VERSION)
        .field(FIELD_NAME, Schema.STRING_SCHEMA)
        .build();
```

## Key Dependencies

- **Kafka Connect API**: `org.apache.kafka:connect-api`
- **Jackson**: `com.fasterxml.jackson.core:*`, `jackson-datatype-jsr310`
- **Guava**: `com.google.guava:guava`
- **AsyncHttpClient**: `org.asynchttpclient:async-http-client`
- **OkHttp**: `com.squareup.okhttp3:okhttp`
- **Failsafe**: `dev.failsafe:failsafe` (retry logic)
- **Testcontainers**: `org.testcontainers:*`
- **WireMock**: `org.wiremock:wiremock`

## Common Tasks

```bash
# Run only core module tests
mvn test -pl http-core

# Run only connector tests
mvn test -pl kafka-connect-http-connectors

# Generate Javadoc
mvn javadoc:javadoc
```

## Additional Notes

- All modules must pass tests before merging
- Integration tests use Testcontainers and may require Docker
- Template processors support various expression languages (JsonPath, XPath, JMESPath, regex)
- Project uses Maven Surefire for unit tests, Failsafe for integration tests
- JaCoCo for code coverage (`-Pcoverage` profile)
