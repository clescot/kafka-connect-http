package io.github.clescot.kafka.connect.http.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dev.failsafe.RetryPolicy;
import io.github.clescot.kafka.connect.MapUtils;
import io.github.clescot.kafka.connect.http.core.HttpExchange;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.jetbrains.annotations.NotNull;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;

import static io.github.clescot.kafka.connect.Configuration.DEFAULT_CONFIGURATION_ID;
import static io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition.*;
import static io.github.clescot.kafka.connect.http.client.HttpClientConfigDefinition.HTTP_CLIENT_UNSECURE_RANDOM_SEED;

public class HttpClientConfigurationFactory {


    private HttpClientConfigurationFactory() {
    }


}
