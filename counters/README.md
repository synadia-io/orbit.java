<img src="../orbit_shorter.png" alt="Orbit">

# Distributed Counters

Utility to take advantage of the distributed counter functionality.

https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-49.md

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:counters-197556?labelColor=grey&style=flat)
![0.2.2](https://img.shields.io/badge/Current_Release-0.2.2-27AAE0)
![0.2.3](https://img.shields.io/badge/Current_Snapshot-0.2.3--SNAPSHOT-27AAE0)
[![Dependencies Help](https://img.shields.io/badge/Dependencies%20Help-27AAE0)](https://github.com/synadia-io/orbit.java?tab=readme-ov-file#dependencies)
[![javadoc](https://javadoc.io/badge2/io.synadia/counters/javadoc.svg)](https://javadoc.io/doc/io.synadia/counters)
[![Maven Central](https://img.shields.io/maven-central/v/io.synadia/counters)](https://img.shields.io/maven-central/v/io.synadia/counters)

## Basic Usage

```java
Options options = ...
try (Connection nc = Nats.connect(options)) {
    JetStreamManagement jsm = nc.jetStreamManagement();

    // setup the coutner stream
    Counters counters = createCountersStream(nc,
        StreamConfiguration.builder()
            .name("counters-stream")
            .subjects("cs.*")
            .storageType(StorageType.Memory)
            .build());

    // add
    BigInteger bi = counters.add("cs.A", 1);
    bi = counters.add("cs.A", 2);

    bi = counters.add("cs.B", 10);
    bi = counters.add("cs.B", 20);

    // get
    bi = counters.get("cs.A");
    bi = counters.get("cs.B");
```


## API

JetStreamOptions are necessary for stream creation and instance construction if your stream needs a prefix or domain.

### Create Counter Stream
```java
public static Counters createCountersStream(Connection conn, StreamConfiguration userConfig) throws JetStreamApiException, IOException
public static Counters createCountersStream(Connection conn, JetStreamOptions jso, StreamConfiguration userConfig) throws JetStreamApiException, IOException
```

### Create Counters Instance
You get a counters instance on construction of the stream as above or by constructing an instance directly.

```java
public Counters(String streamName, Connection conn) throws IOException, JetStreamApiException
public Counters(String streamName, Connection conn, JetStreamOptions jso) throws IOException, JetStreamApiException
```

### Counters instance API
```java
public BigInteger add(String subject, int value) throws JetStreamApiException, IOException
public BigInteger add(String subject, long value) throws JetStreamApiException, IOException
public BigInteger add(String subject, BigInteger value) throws JetStreamApiException, IOException
public BigInteger increment(String subject) throws JetStreamApiException, IOException
public BigInteger decrement(String subject) throws JetStreamApiException, IOException
public BigInteger setViaAdd(String subject, int value) throws JetStreamApiException, IOException
public BigInteger setViaAdd(String subject, long value) throws JetStreamApiException, IOException
public BigInteger setViaAdd(String subject, BigInteger value) throws JetStreamApiException, IOException
public BigInteger get(String subject) throws JetStreamApiException, IOException
public BigInteger getOrElse(String subject, int dflt) throws IOException
public BigInteger getOrElse(String subject, long dflt) throws IOException
public BigInteger getOrElse(String subject, BigInteger dflt) throws IOException
public CounterEntry getEntry(String subject) throws JetStreamApiException, IOException
public LinkedBlockingQueue<CounterEntryResponse> getEntries(String... subjects)
public LinkedBlockingQueue<CounterEntryResponse> getEntries(List<String> subjects)
public CounterIterator iterateEntries(String... subjects)
public CounterIterator iterateEntries(List<String> subjects)
public CounterIterator iterateEntries(List<String> subjects, Duration timeoutFirst, Duration timeoutSubsequent)
```
![Artifact](https://img.shields.io/badge/Artifact-io.synadia:counters-197556?labelColor=grey&style=flat)
[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/counters/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/counters)
[![javadoc](https://javadoc.io/badge2/io.synadia/counters/javadoc.svg)](https://javadoc.io/doc/io.synadia/counters)

---
Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
See [LICENSE](LICENSE) and [NOTICE](NOTICE) file for details.
