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
* `job1.headers` : request's headers list (`Content-Type,If-Modified-Since,Accept-Language`)
* `job1.header.Content-Type: application/json` 
* `job1.header.If-Modified-Since: Mon, 18 Jul 2016 02:36:04 GMT` 
* `job1.header.If-Modified-Since: Mon, 18 Jul 2016 02:36:04 GMT` 
* `job1.header.Cache-Control: max-age=0`

## configuration example

```json
{
   "tasks.max" : "1",
   "connector.class" : "io.github.clescot.kafka.connect.http.source.cron.CronSourceConnector",
   "topic" : "requests",
   "key.converter" : "org.apache.kafka.connect.storage.StringConverter",
   "value.converter" : "org.apache.kafka.connect.storage.StringConverter",
   "jobs" : "job1",
   "job1.url" : "http://mywebsite.com/ping",
   "job1.cron" : "0/5 * * ? * *",
   "job1.method" : "POST",
   "job1.body" : "stuff",
   "job1.headers" : "X-Request-ID,X-Correlation-ID",
   "job1.header.X-Request-ID" : "e6de70d1-f222-46e8-b755-11111",
   "job1.header.X-Correlation-ID" : "e6de70d1-f222-46e8-b755-754880687822"
}
```