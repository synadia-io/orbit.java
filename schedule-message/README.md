<img src="../orbit_shorter.png" alt="Orbit">

# Scheduled Message

Utility to leverage the ability to schedule a message to be published at a later time.
Eventually the ability to schedule a message to publish based on a cron or schedule. 

https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-51.md

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:schedule--message-197556?labelColor=grey&style=flat)
![0.0.3](https://img.shields.io/badge/Current_Release-0.0.3-27AAE0)
![0.0.4](https://img.shields.io/badge/Current_Snapshot-0.0.4--SNAPSHOT-27AAE0)
[![Dependencies Help](https://img.shields.io/badge/Dependencies%20Help-27AAE0)](https://github.com/synadia-io/orbit.java?tab=readme-ov-file#dependencies)
[![javadoc](https://javadoc.io/badge2/io.synadia/schedule-message/javadoc.svg)](https://javadoc.io/doc/io.synadia/schedule-message)
[![Maven Central](https://img.shields.io/maven-central/v/io.synadia/schedule-message)](https://img.shields.io/maven-central/v/io.synadia/schedule-message)

### Building a Scheduled Message

A scheduled message is just a normal message with some extra headers. 

It consists of a subject that holds the schedule message, 
a subject that is the target subject for the schedule,
the scheduling information which can be a specific time or a standard cron based schedule. 

The `ScheduledMessageBuilder` makes it easy to create this using a builder pattern.

### Basic message content

You can add message data and custom headers like a normal message with these builder methods:

```
scheduleSubject(String scheduleSubject) // set the primary subject
targetSubject(String targetSubject)     // set the subject that is the target of the schedule
data(byte[] data)                       // set the data from a byte array
data(String data)                       // set the data from a UTF-8 string
data(String data, Charset charset)      // set the data from a string
headers(Headers headers)                // set user headers
copy(Message message)                   // copy the subject, data and headers from an existing message
```

### Scheduling variations

There are several scheduling variations. Only the last one given to the builder is used.

```
scheduleAt(ZonedDateTime zdt)
scheduleImmediate()
schedule(Predefined predefined)
scheduleEvery(String every)
scheduleCron(String cron)
```

### TTL

You can set a scheduled message to have a TTL

```
messageTtl(MessageTtl messageTtl)
```

### Predefined Schedules 

There is an enum that pre-defines some repeating schedule behavior. 
```
/**
 * Run once a year, midnight, Jan. 1st. Same as Yearly. Equivalent to cron string 0 0 0 1 1 *
 */
Annually("@annually"),

/**
 * Run once a year, midnight, Jan. 1st. Same as Annually. Equivalent to cron string 0 0 0 1 1 *
 */
Yearly("@yearly"),

/**
 * Run once a month, midnight, first of month. Same as cron format 0 0 0 1 * *
 */
Monthly("@monthly"),

/**
 * Run once a week, midnight between Sat/Sun. Equivalent to cron string 0 0 0 * * 0
 */
Weekly("@weekly"),

/**
 * Run once a day, midnight. Same as Daily. Equivalent to cron string 0 0 0 * * *
 */
Midnight("@midnight"),

/**
 * Run once a day, midnight. Same as Midnight. Equivalent to cron string 0 0 0 * * *
 */
Daily("@daily"),

/**
 * Run once an hour, beginning of hour. Equivalent to cron string 0 0 * * * *
 */
Hourly("@hourly");
```

### ADR

The original feature design document: [JetStream Message Scheduler ADR-51](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-51.md)

---
Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
See [LICENSE](LICENSE) and [NOTICE](NOTICE) file for details.
