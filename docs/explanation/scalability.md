# Scalability

Kafka Connect distributes the work of a connector across **tasks** and across the workers of a
cluster.

## Tasks and workers

`tasks.max` sets the maximum number of tasks a connector may create. Kafka Connect spreads
those tasks over the available workers, up to one task per worker at a time. Adding workers
lets a connector scale horizontally; the framework rebalances tasks automatically.

The actual number of tasks can be lower than `tasks.max` when there are fewer workers or when
the connector cannot use them. The framework adapts the count to the available resources, so
scaling is dynamic rather than fixed. Monitor the connector and adjust `tasks.max` as load
changes.

## HTTP Sink connector

The sink connector reads request messages from its input topic. Every connector instance
receives the full configuration, so any instance can handle any request. Load is distributed
by **Kafka partitions**: each instance handles the partitions assigned to it. Adding
partitions to the input topic increases the achievable parallelism.

## SSE Source connector

The SSE source connector opens one stream per configured endpoint. Each instance creates a
task for each configured endpoint, so the parallelism is bounded by the number of endpoints,
not by partitions.

## Cron Source connector

The Cron source connector creates one job per configured cron entry. Each instance creates a
task for each job, so parallelism is bounded by the number of jobs.

## The locality constraint

`IN_MEMORY_QUEUE` mode adds one constraint on top of the above: the sink and source connectors
must be co-located, because they share an in-memory queue (see
[publish modes](publish-modes.md)). Practically:

- use the same `tasks.max` for both connectors;
- use the same partition count for the sink and source topics;
- do not deploy more workers than the partition count.

This keeps request/response correlation local without giving up the distribution of the HTTP
calls themselves.
