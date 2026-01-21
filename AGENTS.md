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

# Skip tests during build
mvn clean install -DskipTests
```

## Project Structure

- **kafka-connect-http-core**: Core library containing HTTP request/response models, template processors, and SSE support
- **kafka-connect-http-connectors**: Kafka Connect sink and source connector implementations
- **report-aggregate**: Aggregated coverage reports

## Code Style Guidelines

### Java Version
- Use Java 17 features (text blocks, pattern matching, records where appropriate)
- Ensure backward compatibility with Java 17 target

### Imports
- Use Guava utility classes for collection creation:
  - `com.google.common.collect.Maps.newHashMap()`
  - `com.google.common.collect.Lists.newArrayList()`
  - `com.google.common.base.Preconditions` for argument validation
  - `com.google.common.base.MoreObjects` for toString/equals helpers
- Group imports in this order:
  1. `io.github.clescot` (project imports)
  2. `org.apache.kafka` (Kafka imports)
  3. `com.fasterxml.jackson` (Jackson imports)
  4. `com.google.guava` (Guava imports)
  5. `org.slf4j` (logging)
  6. Other third-party imports
  7. `java.*` imports
- Use static imports for constants and test assertions

### Naming Conventions
- **Classes**: PascalCase (e.g., `HttpRequest`, `ExchangeTemplateManager`)
- **Methods**: camelCase with underscores for test methods (e.g., `test_serialization_with_attributes`)
- **Constants**: UPPER_SNAKE_CASE (e.g., `URL_FIELD`, `DEFAULT_TIMEOUT`)
- **Variables**: camelCase (e.g., `httpRequest`, `headersMap`)
- **Packages**: lowercase with domain-reversed prefix (e.g., `io.github.clescot.kafka.connect.http.core`)

### Field Declarations
- Use `@Serial` annotation for Serializable class serialVersionUID fields
- Declare static final fields before instance fields
- Initialize collections inline when possible
- Use `Maps.newHashMap()` and `Lists.newArrayList()` instead of `new HashMap<>()` and `new ArrayList<>()`

### Logging
- Use SLF4J: `private static final Logger LOGGER = LoggerFactory.getLogger(ClassName.class);`
- Use parameterized logging: `LOGGER.debug("message {}", variable)`
- Log at appropriate levels (DEBUG for details, INFO for operations, WARN for warnings, ERROR for failures)

### Error Handling
- Use Guava `Preconditions.checkNotNull()` for required argument validation
- Throw `IllegalArgumentException` for invalid inputs
- Wrap checked exceptions in runtime exceptions when appropriate
- Use try-with-resources for closeable resources
- Include context in exception messages

### Serialization
- Use Jackson annotations:
  - `@JsonProperty` for JSON properties
  - `@JsonInclude(Include.NON_EMPTY)` to exclude empty values
  - `@JsonIgnore` for properties to exclude from serialization
- Implement `Serializable` with `@Serial` and `serialVersionUID`
- Provide `toStruct()` method for Kafka Connect Struct conversion
- Use Kafka Connect Schema for struct definitions

### Testing
- Use JUnit 6 (`org.junit.jupiter.api.*`)
- Use AssertJ for assertions: `assertThat(...)`
- Use `@Nested` for organizing related tests
- Test method names: snake_case describing scenario (e.g., `test_serialization_with_multipart_and_file`)
- Use text blocks (`"""..."""`) for JSON test data
- Use `@BeforeEach` for test setup
- Use WireMock for HTTP mocking
- Use Testcontainers for integration testing

### Code Patterns

#### Constructor Validation
```java
public ClassName(String requiredParam, Type optionalParam) {
    Preconditions.checkNotNull(requiredParam, "requiredParam is required");
    this.field = requiredParam;
    this.optionalField = MoreObjects.firstNonNull(optionalParam, defaultValue);
}
```

#### equals/hashCode
```java
@Override
public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ClassName that = (ClassName) o;
    return Objects.equals(field1, that.field1)
            && Objects.equals(field2, that.field2);
}

@Override
public int hashCode() {
    return Objects.hash(field1, field2);
}
```

#### Logger Pattern
```java
private static final Logger LOGGER = LoggerFactory.getLogger(ClassName.class);

public void method() {
    LOGGER.debug("Processing {}", item);
    // implementation
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

## Configuration

The project uses:
- Maven Compiler Plugin with Java 17 target/release
- JaCoCo for code coverage (`-Pcoverage` profile)
- Surefire for unit tests, Failsafe for integration tests
- SonarQube/SonarCloud for quality analysis

## Key Dependencies

- **Kafka Connect API**: `org.apache.kafka:connect-api`
- **Jackson**: `com.fasterxml.jackson.core:*`, `jackson-datatype-jsr310`
- **Guava**: `com.google.guava:guava`
- **AsyncHttpClient**: `org.asynchttpclient:async-http-client`
- **OkHttp**: `com.squareup.okhttp3:okhttp`
- **Failsafe**: `dev.failsafe:failsafe` (retry logic)
- **Testcontainers**: `org.testcontainers:*`
- **WireMock**: `org.wiremock:wiremock`

## Maven Profiles

- **coverage**: Generates JaCoCo coverage reports
- **github**: Configures GitHub Maven package repository
- **sonatypeDeploy**: Configures Central Publishing for Maven Central

## Common Tasks

### Running Specific Test Categories
```bash
# Run only core module tests
mvn test -pl kafka-connect-http-core

# Run only connector tests
mvn test -pl kafka-connect-http-connectors

# Run tests with verbose output
mvn test -X
```

### Building Documentation
```bash
# Generate Javadoc
mvn javadoc:javadoc

# Generate source JARs
mvn source:jar
```

### Code Quality
```bash
# Run static analysis (if configured)
mvn sonar:sonar

# Validate plugins
mvn plugin:validate
```

## IDE Configuration

VS Code settings (`.vscode/settings.json`):
```json
{
    "java.compile.nullAnalysis.mode": "automatic"
}
```

## Additional Notes

- All modules must pass tests before merging
- Integration tests use Testcontainers and may require Docker
- WireMock is used for HTTP mocking in unit tests
- Schema Registry tests use MockSchemaRegistryClient
- Template processors support various expression languages (JsonPath, XPath, JMESPath, regex)
