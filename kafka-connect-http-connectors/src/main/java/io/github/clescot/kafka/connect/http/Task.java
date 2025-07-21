package io.github.clescot.kafka.connect.http;

import io.github.clescot.kafka.connect.http.client.Configuration;

public abstract class Task<C,R,S> {
    public abstract Configuration<C,R> selectConfiguration(R request);

    // This class is a placeholder for the Task class in the Kafka Connect framework.
    // It can be extended to implement specific task functionality for HTTP connectors.

    // The generic types R and S can be used to represent request and response types respectively.
    // This allows for flexibility in defining the types of requests and responses handled by the task.

    // Additional methods and properties can be added as needed to implement specific task behavior.
}
