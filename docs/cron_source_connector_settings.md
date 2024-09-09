# CronSourceConnector

Cron Source connector permits to emit some HttpRequest messages at a pace defined in [a cron expression](https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html) via the [Quartz library](https://www.quartz-scheduler.org).
into a configured topic, which must be the topic listened by the HTTP Sink Connector.

## required parameters

