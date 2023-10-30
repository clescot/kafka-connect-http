package io.github.clescot.kafka.connect.http;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.KafkaJsonSerializerConfig.WRITE_DATES_AS_ISO8601;
import static io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig.FAIL_INVALID_SCHEMA;
import static io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig.FAIL_UNKNOWN_PROPERTIES;
import static io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig.ONEOF_FOR_NULLABLES;
import static io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig.SCHEMA_SPEC_VERSION;

public class JsonSchemaSerdeConfigFactory {


    private final Map<String,Object> serdeConfig = Maps.newHashMap();
    private static final List<String> jsonSchemaVersions =Lists.newArrayList("draft_4","draft_6","draft_7","draft_2019_09");
    public JsonSchemaSerdeConfigFactory(String schemaRegistryUrl,
                                        boolean autoRegisterSchemas,
                                        String jsonSchemaSpecVersion,
                                        boolean writeDatesAsIso8601,
                                        boolean oneOfForNullables,
                                        boolean failInvalidSchema,
                                        boolean failUnknownProperties) {

        serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        serdeConfig.put(AUTO_REGISTER_SCHEMAS, autoRegisterSchemas);
        Preconditions.checkNotNull(jsonSchemaSpecVersion);
        Preconditions.checkArgument(!jsonSchemaSpecVersion.isEmpty(),"'jsonSchemaSpecVersion' must not be an empty string");
        Preconditions.checkArgument(jsonSchemaVersions.contains(jsonSchemaSpecVersion.toLowerCase()),"jsonSchemaSpecVersion supported values are 'draft_4','draft_6','draft_7','draft_2019_09' but not '"+jsonSchemaSpecVersion+"'");
        serdeConfig.put(SCHEMA_SPEC_VERSION, jsonSchemaSpecVersion);
        serdeConfig.put(WRITE_DATES_AS_ISO8601, writeDatesAsIso8601);
        serdeConfig.put(ONEOF_FOR_NULLABLES, oneOfForNullables);
        serdeConfig.put(FAIL_INVALID_SCHEMA, failInvalidSchema);
        serdeConfig.put(FAIL_UNKNOWN_PROPERTIES, failUnknownProperties);
    }

    public Map<String,Object> getConfig(){
        return ImmutableMap.copyOf(serdeConfig);
    }
}
