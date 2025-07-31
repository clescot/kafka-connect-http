# scalability

Kafka connect distribute the load of the connectors among tasks, across the cluster, via the `tasks.max` parameter.
The `tasks.max` parameter is used to define the maximum number of tasks that can be created for a connector.
This parameter is set in the connector configuration and can be adjusted based on the expected load and performance requirements.
The connector framework will automatically distribute the tasks across the available workers in the cluster (instances of the connector).
This allows for horizontal scaling of the connector, as more workers (i.e connector instances) can be added to the cluster to handle increased load.
The `tasks.max` parameter is a key factor in determining the scalability of the connector.
It is important to note that the actual number of tasks created may be less than the value specified in `tasks.max`, 
depending on the number of available workers and the connector's configuration.
The kafka connect framework will automatically adjust the number of tasks based on the available resources and the connector's configuration.
This allows for dynamic scaling of the connector, as the number of tasks can be adjusted based on the current load and performance requirements.
It is recommended to monitor the performance of the connector and adjust the `tasks.max` parameter as needed to ensure optimal performance and scalability.

Internally, the Kafka Connect framework distribute the load with the `taskConfigs` method of the connector class.
This method is responsible for creating the task configurations based on the connector configuration and the number of tasks.
The method will create a list of task configurations, each with its own set of parameters, based
on the connector configuration and the number of tasks.

## the Http Sink Connector

The connector listens to the incoming HttpRequest messages from the incoming Kafka topic.
Each connector instance get all the configurations, to be able to handle all the HttpRequest messages.
The distribution  of the load is done by the repartition of the messages in the Kafka topic among partitions.
Each connector instance will handle the messages from the partitions it is assigned to by the Kafka Connect framework.
This allows for horizontal scaling of the connector, as more partitions can be added to the topic to
handle increased load.

## the Sse Source Connector

The Sse Source Connector is designed to connect to multiple SSE endpoints and listen for events.
Each connector instance will create a task for each configured SSE endpoint.
The distribution of the load is done by the number of tasks created for each connector instance.


# the Cron Source Connector
The Cron Source Connector is designed to generate messages based on a cron expression.
Each connector instance will create a task for each configured cron expression.
The distribution of the load is done by the number of tasks created for each connector instance.
