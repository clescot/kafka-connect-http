package io.github.clescot.kafka.connect.http.sink;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.github.clescot.client.http.HttpClientConfigDefinition;
import io.github.clescot.client.ConfigUtils;
import io.github.clescot.kafka.connect.http.mapper.MapperMode;
import org.apache.kafka.common.config.ConfigDef;

import java.util.*;

import static io.github.clescot.client.Constants.*;


public class HttpConfigDefinition {




    private final Map<String, String> settings;

    public HttpConfigDefinition(Map<String, String> settings) {
        this.settings = settings;
    }


    public ConfigDef config() {
        HttpClientConfigDefinition httpClientConfigDefinition = new HttpClientConfigDefinition(settings);
        ConfigDef configDef = httpClientConfigDefinition.config()
                //meter registry
                //exporters
                .define(METER_REGISTRY_EXPORTER_JMX_ACTIVATE, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_EXPORTER_JMX_ACTIVATE_DOC)
                .define(METER_REGISTRY_EXPORTER_PROMETHEUS_ACTIVATE, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_EXPORTER_PROMETHEUS_ACTIVATE_DOC)
                .define(METER_REGISTRY_EXPORTER_PROMETHEUS_PORT, ConfigDef.Type.INT, 9090, ConfigDef.Importance.LOW, METER_REGISTRY_EXPORTER_PROMETHEUS_PORT_DOC)
                //bind metrics
                .define(METER_REGISTRY_BIND_METRICS_EXECUTOR_SERVICE, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_BIND_METRICS_EXECUTOR_SERVICE_DOC)
                .define(METER_REGISTRY_BIND_METRICS_JVM_CLASSLOADER, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_BIND_METRICS_JVM_CLASSLOADER_DOC)
                .define(METER_REGISTRY_BIND_METRICS_JVM_PROCESSOR, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_BIND_METRICS_JVM_PROCESSOR_DOC)
                .define(METER_REGISTRY_BIND_METRICS_JVM_GC, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_BIND_METRICS_JVM_GC_DOC)
                .define(METER_REGISTRY_BIND_METRICS_JVM_INFO, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_BIND_METRICS_JVM_INFO_DOC)
                .define(METER_REGISTRY_BIND_METRICS_JVM_MEMORY, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_BIND_METRICS_JVM_MEMORY_DOC)
                .define(METER_REGISTRY_BIND_METRICS_JVM_THREAD, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_BIND_METRICS_JVM_THREAD_DOC)
                .define(METER_REGISTRY_BIND_METRICS_LOGBACK, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_BIND_METRICS_LOGBACK_DOC)
                //tags
                .define(METER_REGISTRY_TAG_INCLUDE_LEGACY_HOST, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_TAG_INCLUDE_LEGACY_HOST_DOC)
                .define(METER_REGISTRY_TAG_INCLUDE_URL_PATH, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, METER_REGISTRY_TAG_INCLUDE_URL_PATH_DOC)

                .define(USER_AGENT_OVERRIDE, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.LOW, CONFIG_DEFAULT_USER_AGENT_OVERRIDE_DOC)
                .define(USER_AGENT_CUSTOM_VALUES, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_USER_AGENT_CUSTOM_VALUES_DOC)
                //async settings
                .define(HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE, ConfigDef.Type.INT, null, ConfigDef.Importance.MEDIUM, HTTP_CLIENT_ASYNC_FIXED_THREAD_POOL_SIZE_DOC)

                //custom message splitters
                .define(MESSAGE_SPLITTER_IDS, ConfigDef.Type.LIST, Lists.newArrayList(), ConfigDef.Importance.LOW, MESSAGE_SPLITTER_IDS_DOC)
                //custom request groupers
                .define(REQUEST_GROUPER_IDS, ConfigDef.Type.LIST, Lists.newArrayList(), ConfigDef.Importance.LOW, REQUEST_GROUPER_IDS_DOC)
                //custom request mappers
                .define(HTTP_REQUEST_MAPPER_IDS, ConfigDef.Type.LIST, Lists.newArrayList(), ConfigDef.Importance.LOW, HTTP_REQUEST_MAPPER_IDS_DOC);
                SinkConfigDefinition sinkConfigDefinition = new SinkConfigDefinition();
                configDef = ConfigUtils.mergeConfigDefs(configDef, sinkConfigDefinition.config());

        //custom httpRequestmappers
        String httpRequestMapperIds = settings.get(HTTP_REQUEST_MAPPER_IDS);
        Set<String> mappers = Sets.newHashSet();
        if (httpRequestMapperIds != null) {
            mappers.addAll(Arrays.asList(httpRequestMapperIds.split(",")));
        }
        mappers.add("default");
        for (String httpRequestmapperName : mappers) {
            configDef = appendHttpRequestMapperConfigDef(configDef, httpRequestmapperName);
        }

        //DNS over HTTPS (DoH)
        configDef = configDef.define(OKHTTP_DOH_ACTIVATE, ConfigDef.Type.BOOLEAN, Boolean.FALSE, ConfigDef.Importance.LOW, OKHTTP_DOH_ACTIVATE_DOC)
                .define(OKHTTP_DOH_BOOTSTRAP_DNS_HOSTS, ConfigDef.Type.LIST, null, ConfigDef.Importance.LOW, OKHTTP_DOH_BOOTSTRAP_DNS_HOSTS_DOC)
                .define(OKHTTP_DOH_INCLUDE_IPV6, ConfigDef.Type.BOOLEAN, Boolean.TRUE, ConfigDef.Importance.LOW, OKHTTP_DOH_INCLUDE_IPV6_DOC)
                .define(OKHTTP_DOH_USE_POST_METHOD, ConfigDef.Type.BOOLEAN, Boolean.FALSE, ConfigDef.Importance.LOW, OKHTTP_DOH_USE_POST_METHOD_DOC)
                .define(OKHTTP_DOH_RESOLVE_PRIVATE_ADDRESSES, ConfigDef.Type.BOOLEAN, Boolean.FALSE, ConfigDef.Importance.LOW, OKHTTP_DOH_RESOLVE_PRIVATE_ADDRESSES_DOC)
                .define(OKHTTP_DOH_RESOLVE_PUBLIC_ADDRESSES, ConfigDef.Type.BOOLEAN, Boolean.TRUE, ConfigDef.Importance.LOW, OKHTTP_DOH_RESOLVE_PUBLIC_ADDRESSES_DOC)
                .define(OKHTTP_DOH_URL, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, OKHTTP_DOH_URL_DOC);
        //custom configurations
        String configurationIds = settings.get(CONFIGURATION_IDS);
        Set<String> configs = Sets.newHashSet();
        if (configurationIds != null) {
            configs.addAll(Arrays.asList(configurationIds.split(",")));
        }
        configs.add("default");
        for (String configurationName : configs) {
            configDef = appendConfigurationConfigDef(configDef, configurationName);
        }
        return configDef;
    }

    private ConfigDef appendConfigurationConfigDef(ConfigDef configDef, String configurationName) {
        String prefix = "config." + configurationName + ".";
        ConfigDef configDef1 = configDef//http client implementation settings
                //cookie policy settings
                .define(prefix + HTTP_COOKIE_POLICY, ConfigDef.Type.STRING, CONFIG_DEFAULT_HTTP_COOKIE_POLICY, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_COOKIE_POLICY_DOC)
                //retry after settings
                .define(prefix + RETRY_AFTER_MAX_DURATION_IN_SEC,ConfigDef.Type.STRING, DEFAULT_RETRY_AFTER_MAX_DURATION_IN_SEC ,ConfigDef.Importance.LOW, CONFIG_DEFAULT_RETRY_AFTER_MAX_DURATION_IN_SEC_DOC)
                .define(prefix + RETRY_DELAY_THRESHOLD_IN_SEC,ConfigDef.Type.STRING, DEFAULT_RETRY_DELAY_THRESHOLD_IN_SEC ,ConfigDef.Importance.LOW, CONFIG_DEFAULT_RETRY_DELAY_THRESHOLD_IN_SEC_DOC)
                .define(prefix + DEFAULT_RETRY_DELAY_IN_SEC,ConfigDef.Type.STRING, DEFAULT_DEFAULT_RETRY_DELAY_IN_SEC ,ConfigDef.Importance.LOW, CONFIG_DEFAULT_DEFAULT_RETRY_DELAY_IN_SEC_DOC)
                .define(prefix + CUSTOM_STATUS_CODE_FOR_RETRY_AFTER_HEADER,ConfigDef.Type.STRING, DEFAULT_CUSTOM_STATUS_CODE_FOR_RETRY_AFTER_HEADER ,ConfigDef.Importance.LOW, CONFIG_DEFAULT_RETRY_DELAY_THRESHOLD_IN_SEC_DOC)
                //retry settings
                .define(prefix + SUCCESS_RESPONSE_CODE_REGEX, ConfigDef.Type.STRING, CONFIG_DEFAULT_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX, ConfigDef.Importance.LOW, CONFIG_DEFAULT_SUCCESS_RESPONSE_CODE_REGEX_DOC)
                .define(prefix + RETRY_RESPONSE_CODE_REGEX, ConfigDef.Type.STRING, DEFAULT_DEFAULT_RETRY_RESPONSE_CODE_REGEX, ConfigDef.Importance.LOW, DEFAULT_RETRY_RESPONSE_CODE_REGEX_DOC)
                .define(prefix + RETRIES, ConfigDef.Type.INT, DEFAULT_RETRIES_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RETRIES_DOC)
                .define(prefix + RETRY_DELAY_IN_MS, ConfigDef.Type.LONG, DEFAULT_RETRY_DELAY_IN_MS_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RETRY_DELAY_IN_MS_DOC)
                .define(prefix + RETRY_MAX_DELAY_IN_MS, ConfigDef.Type.LONG, DEFAULT_RETRY_MAX_DELAY_IN_MS_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RETRY_MAX_DELAY_IN_MS_DOC)
                .define(prefix + RETRY_DELAY_FACTOR, ConfigDef.Type.DOUBLE, DEFAULT_RETRY_DELAY_FACTOR_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RETRY_DELAY_FACTOR_DOC)
                .define(prefix + RETRY_JITTER_IN_MS, ConfigDef.Type.LONG, DEFAULT_RETRY_JITTER_IN_MS_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RETRY_JITTER_IN_MS_DOC)
                //rate limiting settings
                .define(prefix + RATE_LIMITER_PERIOD_IN_MS, ConfigDef.Type.LONG, DEFAULT_RATE_LIMITER_PERIOD_IN_MS_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RATE_LIMITER_PERIOD_IN_MS_DOC)
                .define(prefix + RATE_LIMITER_MAX_EXECUTIONS, ConfigDef.Type.LONG, DEFAULT_RATE_LIMITER_MAX_EXECUTIONS_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RATE_LIMITER_MAX_EXECUTIONS_DOC)
                .define(prefix + RATE_LIMITER_SCOPE, ConfigDef.Type.STRING, DEFAULT_RATE_LIMITER_SCOPE_VALUE, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RATE_LIMITER_SCOPE_DOC)
                .define(prefix + RATE_LIMITER_PERMITS_PER_EXECUTION, ConfigDef.Type.STRING, DEFAULT_RATE_LIMITER_ONE_PERMIT_PER_CALL, ConfigDef.Importance.MEDIUM, CONFIG_DEFAULT_RATE_LIMITER_PERMITS_PER_EXECUTION_DOC)
                //header settings
                .define(prefix + STATIC_REQUEST_HEADER_NAMES, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.MEDIUM, CONFIG_STATIC_REQUEST_HEADER_NAMES_DOC)
                .define(prefix + GENERATE_MISSING_CORRELATION_ID, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.MEDIUM, CONFIG_GENERATE_MISSING_CORRELATION_ID_DOC)
                .define(prefix + GENERATE_MISSING_REQUEST_ID, ConfigDef.Type.STRING, FALSE, ConfigDef.Importance.MEDIUM, CONFIG_GENERATE_MISSING_REQUEST_ID_DOC)
                .define(prefix + USER_AGENT_OVERRIDE, ConfigDef.Type.STRING, "http_client", ConfigDef.Importance.LOW, CONFIG_DEFAULT_USER_AGENT_OVERRIDE_DOC)
                .define(prefix + USER_AGENT_CUSTOM_VALUES, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_DEFAULT_USER_AGENT_CUSTOM_VALUES_DOC)

                //http response
                .define(prefix + HTTP_RESPONSE_MESSAGE_STATUS_LIMIT, ConfigDef.Type.INT, 1024, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_RESPONSE_MESSAGE_STATUS_LIMIT_DOC)
                .define(prefix + HTTP_RESPONSE_HEADERS_LIMIT, ConfigDef.Type.INT, 10_000, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_RESPONSE_HEADERS_LIMIT_DOC)
                .define(prefix + HTTP_RESPONSE_BODY_LIMIT, ConfigDef.Type.INT, 100_000, ConfigDef.Importance.LOW, CONFIG_DEFAULT_HTTP_RESPONSE_BODY_LIMIT_DOC);
        String staticHeaderNames = settings.get(prefix + STATIC_REQUEST_HEADER_NAMES);
        if (staticHeaderNames != null && !staticHeaderNames.isBlank()) {
            List<String> staticHeaders = Arrays.asList(staticHeaderNames.split(","));
            for (String staticHeader : staticHeaders) {
                configDef = configDef.define(prefix + "enrich.request.static.header." + staticHeader, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, CONFIG_STATIC_REQUEST_HEADER_NAMES_DOC);
            }
        }
        return configDef1;
    }

    private ConfigDef appendHttpRequestMapperConfigDef(ConfigDef configDef, String httpRequestMapperName) {
        String prefix = "http.request.mapper." + httpRequestMapperName + ".";
        return configDef
                .define(prefix + REQUEST_MAPPER_DEFAULT_MODE, ConfigDef.Type.STRING, MapperMode.DIRECT.name(), ConfigDef.Importance.MEDIUM, REQUEST_MAPPER_DEFAULT_MODE_DOC)
                .define(prefix + REQUEST_MAPPER_DEFAULT_URL_EXPRESSION, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, REQUEST_MAPPER_DEFAULT_URL_EXPRESSION_DOC)
                .define(prefix + REQUEST_MAPPER_DEFAULT_METHOD_EXPRESSION, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, REQUEST_MAPPER_DEFAULT_METHOD_EXPRESSION_DOC)
                .define(prefix + REQUEST_MAPPER_DEFAULT_BODYTYPE_EXPRESSION, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, REQUEST_MAPPER_DEFAULT_BODYTYPE_EXPRESSION_DOC)
                .define(prefix + REQUEST_MAPPER_DEFAULT_BODY_EXPRESSION, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, REQUEST_MAPPER_DEFAULT_BODY_EXPRESSION_DOC)
                .define(prefix + REQUEST_MAPPER_DEFAULT_HEADERS_EXPRESSION, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, REQUEST_MAPPER_DEFAULT_HEADERS_EXPRESSION_DOC)
                .define(prefix + REQUEST_MAPPER_DEFAULT_SPLIT_PATTERN, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, REQUEST_MAPPER_DEFAULT_SPLIT_PATTERN_DOC)
                .define(prefix + REQUEST_MAPPER_DEFAULT_SPLIT_LIMIT, ConfigDef.Type.INT, 0, ConfigDef.Importance.MEDIUM, REQUEST_MAPPER_DEFAULT_SPLIT_LIMIT_DOC);
    }


}
