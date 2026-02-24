<img src="../orbit_shorter.png" alt="Orbit">

# Request-Many Utility

The Request Many utility is a full implementation for an often asked for feature, 
which is the ability to get many responses from a single core request, 
instead of the usual first response for a request. This allows you to implement patterns like:

* Scatter-gather pattern, getting responses from many workers.
* Many responses, for instance, a multipart payload, from a single worker.  

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:request--many-197556?labelColor=grey&style=flat)
![0.1.1](https://img.shields.io/badge/Current_Release-0.1.1-27AAE0)
![0.1.2](https://img.shields.io/badge/Current_Snapshot-0.1.2--SNAPSHOT-27AAE0)
[![Dependencies Help](https://img.shields.io/badge/Dependencies%20Help-27AAE0)](https://github.com/synadia-io/orbit.java?tab=readme-ov-file#dependencies)
[![javadoc](https://javadoc.io/badge2/io.synadia/request-many/javadoc.svg)](https://javadoc.io/doc/io.synadia/request-many)
[![Maven Central](https://img.shields.io/maven-central/v/io.synadia/request-many)](https://img.shields.io/maven-central/v/io.synadia/request-many)

### Request Many Usage

**Fetch** 
* [FetchExample.java](src/examples/java/io/synadia/examples/FetchExample.java)

Fetch returns all the responses in a list: 

**Queue**

* [QueueExample.java](src/examples/java/io/synadia/examples/QueueExample.java)

Queue returns a LinkedBlockingQueue that you can get messages from.

**Request**

* [RequestExample.java](src/examples/java/io/synadia/examples/RequestExample.java)

Request accepts a callback. Fetch and Queue use this under the covers. 
You can use this to provide a custom "sentinel", a message that indicates the request is complete. 
Useful for the multipart payload.

The [Unit Tests](src/test/java/io/synadia/jnats/extension/RetrierTests.java) may also be of interest.

---
Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
See [LICENSE](LICENSE) and [NOTICE](NOTICE) file for details.
