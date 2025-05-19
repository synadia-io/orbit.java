![Synadia](src/main/javadoc/images/synadia-logo.png) &nbsp;&nbsp;&nbsp;&nbsp; ![NATS](src/main/javadoc/images/large-logo.png)

# Request-Many Utility

The Request Many utility is a full implementation for an often asked for feature, 
which is the ability to get many responses from a single core request, 
instead of the usual first response for a request. This allows you to implement patterns like:

* Scatter-gather pattern, getting responses from many workers.
* Many responses, for instance, a multipart payload, from a single worker.  

**Current Release**: 0.1.0
&nbsp; **Current Snapshot**: 0.1.1-SNAPSHOT
&nbsp; **Gradle and Maven** `io.synadia:request-many`
[Dependencies Help](https://github.com/synadia-io/orbit.java?tab=readme-ov-file#dependencies)

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:retrier-00BC8E?labelColor=grey&style=flat)
[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/retrier/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/retrier)
[![javadoc](https://javadoc.io/badge2/io.synadia/retrier/javadoc.svg)](https://javadoc.io/doc/io.synadia/retrier)

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
