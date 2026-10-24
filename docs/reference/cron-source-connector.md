# Cron Source connector reference

Reference for `io.github.clescot.kafka.connect.http.source.cron.CronSourceConnector`.

This connector emits HTTP request messages on a
[Quartz cron](https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html)
schedule to a topic, typically the topic an HTTP Sink connector listens to. It never calls the
HTTP server itself; the sink connector does.

## Required settings

| Setting | Description |
| --- | --- |
| `connector.class` | `io.github.clescot.kafka.connect.http.source.cron.CronSourceConnector` |
| `topic` | Topic that receives the emitted `HttpRequest` messages. |
| `jobs` | Comma-separated list of job ids, e.g. `job1,job2`. |

## Per-job settings

Each id listed in `jobs` needs a `job.<id>.*` block.

| Setting | Required | Description |
| --- | --- | --- |
| `job.<id>.url` | yes | URL to request. |
| `job.<id>.cron` | yes | Quartz cron expression. |
| `job.<id>.method` | no | `GET` (default), `POST`, `PUT`, `PATCH`, `DELETE`, `HEAD`, `OPTIONS`, `TRACE`, `CONNECT`. |
| `job.<id>.body` | no | Request body. |
| `job.<id>.headers` | no | Comma-separated header names to send. |
| `job.<id>.header.<Name>` | no | Value for each header named in `job.<id>.headers`. |

> The prefix is `job.<id>.`, not `<id>.`. For a job called `job1`, the URL is
> `job.job1.url`.

## Cron syntax

Quartz cron expressions have **six or seven** fields (`seconds` first), unlike the five of
Unix cron:

| Expression | Meaning |
| --- | --- |
| `0 0 12 * * ?` | 12:00 every day. |
| `0 0/5 14 * * ?` | Every 5 minutes from 14:00 to 14:55 every day. |
| `0 10,44 14 ? 3 WED` | 14:10 and 14:44 every Wednesday in March. |

## Example

```json
{
  "name": "my-cron-source-connector",
  "config": {
    "connector.class": "io.github.clescot.kafka.connect.http.source.cron.CronSourceConnector",
    "tasks.max": "1",
    "topic": "http-request",
    "jobs": "job1",
    "job.job1.url": "http://mywebsite.com/ping",
    "job.job1.cron": "0/5 * * ? * *",
    "job.job1.method": "POST",
    "job.job1.body": "stuff",
    "job.job1.headers": "X-Request-ID,X-Correlation-ID",
    "job.job1.header.X-Request-ID": "e6de70d1-f222-46e8-b755-754880687822",
    "job.job1.header.X-Correlation-ID": "e6de70d1-f222-46e8-b755-754880687822"
  }
}
```

Each job becomes a work unit that Kafka Connect distributes across tasks.
