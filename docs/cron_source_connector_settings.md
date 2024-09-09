# CronSourceConnector

Cron Source connector permits to emit some HttpRequest messages at a pace defined in [a cron expression](https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html) via the [Quartz library](https://www.quartz-scheduler.org).
into a configured topic, which must be the topic listened by the HTTP Sink Connector.

## required parameters

* `topic` will receive HttpRequest messages emitted
*  `jobs` list of job ids (`job1,job2,job3`).
*  `job1.cron` [cron Quartz expression](https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html) : 
    * `0 0 12 * * ?` : Fire at 12pm (noon) every day
    * `0 0/5 14 * * ?` : Fire every 5 minutes starting at 2pm and ending at 2:55pm, every day
    * `0 10,44 14 ? 3 WED` : Fire at 2:10pm and at 2:44pm every Wednesday in the month of March.
*  `job1.url` 

## optional parameters

* `job1.method` : `CONNECT`,`DELETE`,`GET`,`HEAD`,`PATCH`,`POST`,`PUT`,`OPTIONS`,`TRACE` are supported. if Not set,`GET` is implicitly configured.
* `job1.body`  : string to submit as request's body.
* 