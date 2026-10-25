package io.github.clescot.kafka.connect.http.sink;

import io.github.clescot.kafka.connect.http.sink.publish.PublishMode;
import org.apache.kafka.common.config.ConfigDef;

public class SinkConfigDefinition {

  // producer
  public static final String PRODUCER_PREFIX = "producer.";
  public static final String JSON_PREFIX = "json.";
  public static final String PRODUCER_BOOTSTRAP_SERVERS = PRODUCER_PREFIX + "bootstrap.servers";
  public static final String PRODUCER_BOOTSTRAP_SERVERS_DOC =
      "low level producer bootstrap server adresse to publish";
  public static final String PRODUCER_SUCCESS_TOPIC = PRODUCER_PREFIX + "success.topic";
  public static final String PRODUCER_DLQ_TOPIC = PRODUCER_PREFIX + "dlq.topic";
  public static final String PRODUCER_DEAD_LETTER_QUEUE_TOPIC = PRODUCER_PREFIX + "dlq.topic";
  public static final String PRODUCER_ERROR_TOPIC = PRODUCER_PREFIX + "error.topic";
  public static final String PRODUCER_SUCCESS_TOPIC_DOC = "producer topic when success";
  public static final String PRODUCER_DLQ_TOPIC_DOC = "producer topic when retry delay is too long";
  public static final String PRODUCER_ERROR_TOPIC_DOC = "producer topic when error";
  public static final String PRODUCER_FORMAT = PRODUCER_PREFIX + "format";
  public static final String PRODUCER_FORMAT_DOC =
      "can be either 'json', or 'string'; default to 'string'.";
  public static final String PRODUCER_FORMAT_JSON_PREFIX = PRODUCER_FORMAT + JSON_PREFIX;
  public static final String PRODUCER_JSON_INDENT_OUTPUT =
      PRODUCER_FORMAT_JSON_PREFIX + "indent.output";
  public static final String PRODUCER_FORMAT_JSON_INDENT_OUTPUT_DOC =
      "'true' to indent output, 'false' otherwise. default is 'false'.";

  public static final String PRODUCER_CONTENT = PRODUCER_PREFIX + "content";
  public static final String PRODUCER_CONTENT_DOC =
      "can be either 'exchange' (HttpExchange), or 'response' (HttpResponse); default to 'exchange'.";
  public static final String PRODUCER_SCHEMA_REGISTRY_URL = PRODUCER_PREFIX + "schema.registry.url";
  public static final String PRODUCER_SCHEMA_REGISTRY_URL_DOC =
      "url and port of the schema registry.";
  public static final String PRODUCER_SCHEMA_REGISTRY_CACHE_CAPACITY =
      PRODUCER_PREFIX + "schema.registry.cache.capacity";
  public static final String PRODUCER_SCHEMA_REGISTRY_CACHE_CAPACITY_DOC = "";
  public static final String PRODUCER_SCHEMA_REGISTRY_AUTO_REGISTER =
      PRODUCER_PREFIX + "schema.registry.auto.register";
  public static final String PRODUCER_SCHEMA_REGISTRY_AUTO_REGISTER_DOC = "";
  public static final String PRODUCER_JSON_SCHEMA_SPEC_VERSION =
      PRODUCER_PREFIX + JSON_PREFIX + "schema.spec.version";
  public static final String PRODUCER_JSON_SCHEMA_SPEC_VERSION_DOC = "";
  public static final String PRODUCER_JSON_WRITE_DATES_AS_ISO_8601 =
      PRODUCER_PREFIX + JSON_PREFIX + "write.dates.as.iso.8601";
  public static final String PRODUCER_JSON_WRITE_DATES_AS_ISO_8601_DOC =
      "'true' to write dates in 8601 format, 'false' otherwise. default is 'true'.";
  public static final String PRODUCER_JSON_ONE_OF_FOR_NULLABLES =
      PRODUCER_PREFIX + JSON_PREFIX + "one.of.for.nullables";
  public static final String PRODUCER_JSON_ONE_OF_FOR_NULLABLES_DOC = "";
  public static final String PRODUCER_JSON_FAIL_INVALID_SCHEMA =
      PRODUCER_PREFIX + JSON_PREFIX + "fail.invalid.schema";
  public static final String PRODUCER_JSON_FAIL_INVALID_SCHEMA_DOC = "";
  public static final String PRODUCER_JSON_FAIL_UNKNOWN_PROPERTIES =
      PRODUCER_PREFIX + JSON_PREFIX + "fail.unknown.properties";
  public static final String PRODUCER_JSON_FAIL_UNKNOWN_PROPERTIES_DOC = "";
  public static final String PRODUCER_KEY_SUBJECT_NAME_STRATEGY =
      PRODUCER_PREFIX + JSON_PREFIX + "key.subject.name.strategy";
  public static final String PRODUCER_KEY_SUBJECT_NAME_STRATEGY_DOC = "";
  public static final String PRODUCER_VALUE_SUBJECT_NAME_STRATEGY =
      PRODUCER_PREFIX + JSON_PREFIX + "value.subject.name.strategy";
  public static final String PRODUCER_VALUE_SUBJECT_NAME_STRATEGY_DOC = "";
  public static final String PRODUCER_MISSING_ID_CACHE_TTL_SEC =
      PRODUCER_PREFIX + "missing.id.cache.ttl.sec";
  public static final String PRODUCER_MISSING_ID_CACHE_TTL_SEC_DOC = "";
  public static final String PRODUCER_MISSING_VERSION_CACHE_TTL_SEC =
      PRODUCER_PREFIX + "missing.version.cache.ttl.sec";
  public static final String PRODUCER_MISSING_VERSION_CACHE_TTL_SEC_DOC = "";
  public static final String PRODUCER_MISSING_SCHEMA_CACHE_TTL_SEC =
      PRODUCER_PREFIX + "missing.schema.cache.ttl.sec";
  public static final String PRODUCER_MISSING_SCHEMA_CACHE_TTL_SEC_DOC = "";
  public static final String PRODUCER_MISSING_CACHE_SIZE = PRODUCER_PREFIX + "missing.cache.size";
  public static final String PRODUCER_MISSING_CACHE_SIZE_DOC = "";
  public static final String PRODUCER_BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS =
      PRODUCER_PREFIX + "bearer.auth.cache.expiry.buffer.seconds";
  public static final String PRODUCER_BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS_DOC = "";
  public static final String PRODUCER_BEARER_AUTH_SCOPE_CLAIM_NAME =
      PRODUCER_PREFIX + "bearer.auth.scope.claim.name";
  public static final String PRODUCER_BEARER_AUTH_SCOPE_CLAIM_NAME_DOC = "";
  public static final String PRODUCER_BEARER_AUTH_SUB_CLAIM_NAME =
      PRODUCER_PREFIX + "bearer.auth.sub.claim.name";
  public static final String PRODUCER_BEARER_AUTH_SUB_CLAIM_NAME_DOC = "";
  public static final String PRODUCER_SUCCESS_DEFAULT_TOPIC = "http-success";
  public static final String PRODUCER_DLQ_DEFAULT_TOPIC = "http-dlq";
  public static final String PRODUCER_ERROR_DEFAULT_TOPIC = "http-errors";
  // publish mode
  public static final String PUBLISH_MODE = "publish.mode";
  public static final String PUBLISH_MODE_DOC =
      "can be either 'NONE' or 'PRODUCER'. When set to 'NONE', the connector ignores HTTP responses. When set to 'PRODUCER', the connector publishes each HTTP exchange to another topic with a low-level producer.";

  public ConfigDef config() {
    return new ConfigDef()
        // producer
        // bootstrap servers
        .define(
            PRODUCER_BOOTSTRAP_SERVERS,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.MEDIUM,
            PRODUCER_BOOTSTRAP_SERVERS_DOC)
        .define(
            PRODUCER_SUCCESS_TOPIC,
            ConfigDef.Type.STRING,
            PRODUCER_SUCCESS_DEFAULT_TOPIC,
            ConfigDef.Importance.MEDIUM,
            PRODUCER_SUCCESS_TOPIC_DOC)
        .define(
            PRODUCER_DLQ_TOPIC,
            ConfigDef.Type.STRING,
            PRODUCER_DLQ_DEFAULT_TOPIC,
            ConfigDef.Importance.MEDIUM,
            PRODUCER_DLQ_TOPIC_DOC)
        .define(
            PRODUCER_ERROR_TOPIC,
            ConfigDef.Type.STRING,
            PRODUCER_ERROR_DEFAULT_TOPIC,
            ConfigDef.Importance.MEDIUM,
            PRODUCER_ERROR_TOPIC_DOC)
        .define(
            PRODUCER_KEY_SUBJECT_NAME_STRATEGY,
            ConfigDef.Type.STRING,
            "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy",
            ConfigDef.Importance.MEDIUM,
            PRODUCER_KEY_SUBJECT_NAME_STRATEGY_DOC)
        .define(
            PRODUCER_VALUE_SUBJECT_NAME_STRATEGY,
            ConfigDef.Type.STRING,
            "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy",
            ConfigDef.Importance.MEDIUM,
            PRODUCER_VALUE_SUBJECT_NAME_STRATEGY_DOC)
        .define(
            PRODUCER_MISSING_ID_CACHE_TTL_SEC,
            ConfigDef.Type.LONG,
            null,
            ConfigDef.Importance.LOW,
            PRODUCER_MISSING_ID_CACHE_TTL_SEC_DOC)
        .define(
            PRODUCER_MISSING_VERSION_CACHE_TTL_SEC,
            ConfigDef.Type.LONG,
            null,
            ConfigDef.Importance.LOW,
            PRODUCER_MISSING_VERSION_CACHE_TTL_SEC_DOC)
        .define(
            PRODUCER_MISSING_SCHEMA_CACHE_TTL_SEC,
            ConfigDef.Type.LONG,
            null,
            ConfigDef.Importance.LOW,
            PRODUCER_MISSING_SCHEMA_CACHE_TTL_SEC_DOC)
        .define(
            PRODUCER_MISSING_CACHE_SIZE,
            ConfigDef.Type.INT,
            null,
            ConfigDef.Importance.LOW,
            PRODUCER_MISSING_CACHE_SIZE_DOC)
        .define(
            PRODUCER_BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS,
            ConfigDef.Type.INT,
            null,
            ConfigDef.Importance.LOW,
            PRODUCER_BEARER_AUTH_CACHE_EXPIRY_BUFFER_SECONDS_DOC)
        .define(
            PRODUCER_BEARER_AUTH_SCOPE_CLAIM_NAME,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.LOW,
            PRODUCER_BEARER_AUTH_SCOPE_CLAIM_NAME_DOC)
        .define(
            PRODUCER_BEARER_AUTH_SUB_CLAIM_NAME,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.LOW,
            PRODUCER_BEARER_AUTH_SUB_CLAIM_NAME_DOC)
        // schema registry
        .define(
            PRODUCER_SCHEMA_REGISTRY_URL,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.LOW,
            PRODUCER_SCHEMA_REGISTRY_URL_DOC)
        .define(
            PRODUCER_SCHEMA_REGISTRY_CACHE_CAPACITY,
            ConfigDef.Type.INT,
            1000,
            ConfigDef.Importance.LOW,
            PRODUCER_SCHEMA_REGISTRY_CACHE_CAPACITY_DOC)
        .define(
            PRODUCER_SCHEMA_REGISTRY_AUTO_REGISTER,
            ConfigDef.Type.BOOLEAN,
            Boolean.TRUE,
            ConfigDef.Importance.LOW,
            PRODUCER_SCHEMA_REGISTRY_AUTO_REGISTER_DOC)
        // content
        .define(
            PRODUCER_CONTENT,
            ConfigDef.Type.STRING,
            "exchange",
            ConfigDef.Importance.LOW,
            PRODUCER_CONTENT_DOC)
        // formats
        .define(
            PRODUCER_FORMAT,
            ConfigDef.Type.STRING,
            "string",
            ConfigDef.Importance.LOW,
            PRODUCER_FORMAT_DOC)
        // json
        .define(
            PRODUCER_JSON_SCHEMA_SPEC_VERSION,
            ConfigDef.Type.STRING,
            "draft_2019_09",
            ConfigDef.Importance.LOW,
            PRODUCER_JSON_SCHEMA_SPEC_VERSION_DOC)
        .define(
            PRODUCER_JSON_WRITE_DATES_AS_ISO_8601,
            ConfigDef.Type.BOOLEAN,
            Boolean.TRUE,
            ConfigDef.Importance.LOW,
            PRODUCER_JSON_WRITE_DATES_AS_ISO_8601_DOC)
        .define(
            PRODUCER_JSON_INDENT_OUTPUT,
            ConfigDef.Type.BOOLEAN,
            Boolean.FALSE,
            ConfigDef.Importance.LOW,
            PRODUCER_FORMAT_JSON_INDENT_OUTPUT_DOC)
        .define(
            PRODUCER_JSON_ONE_OF_FOR_NULLABLES,
            ConfigDef.Type.BOOLEAN,
            Boolean.TRUE,
            ConfigDef.Importance.LOW,
            PRODUCER_JSON_ONE_OF_FOR_NULLABLES_DOC)
        .define(
            PRODUCER_JSON_FAIL_INVALID_SCHEMA,
            ConfigDef.Type.BOOLEAN,
            Boolean.TRUE,
            ConfigDef.Importance.LOW,
            PRODUCER_JSON_FAIL_INVALID_SCHEMA_DOC)
        .define(
            PRODUCER_JSON_FAIL_UNKNOWN_PROPERTIES,
            ConfigDef.Type.BOOLEAN,
            Boolean.TRUE,
            ConfigDef.Importance.LOW,
            PRODUCER_JSON_FAIL_UNKNOWN_PROPERTIES_DOC)
        // publish mode
        .define(
            PUBLISH_MODE,
            ConfigDef.Type.STRING,
            PublishMode.NONE.name(),
            ConfigDef.Importance.MEDIUM,
            PUBLISH_MODE_DOC);
  }
}
