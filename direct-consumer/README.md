![Synadia](src/main/javadoc/images/synadia-logo.png) &nbsp;&nbsp;&nbsp;&nbsp; ![NATS](src/main/javadoc/images/large-logo.png)

# Direct Consumer

DIRECT CONSUMER IS PURELY EXPERIMENTAL. IT ONLY WORKS WITH THE 2.11.x NATS SERVER 
and the JNATS 2.20.5.main-2-11-SNAPSHOT 

The direct consumer behaves similar to a JetStream consumer but uses the direct api under the covers. 

**Current Release**: N/A
&nbsp; **Current Snapshot**: 0.1.0-SNAPSHOT
&nbsp; **Gradle and Maven** `io.synadia:direct-consume`
[Dependencies Help](https://github.com/synadia-io/orbit.java?tab=readme-ov-file#dependencies)

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:direct--consumer-00BC8E?labelColor=grey&style=flat)
[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/direct-consumer/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/direct-consumer)
[![javadoc](https://javadoc.io/badge2/io.synadia/direct-consumer/javadoc.svg)](https://javadoc.io/doc/io.synadia/direct-consumer)


# !! IMPORTANT !!

This project is in progress. Please do not use yet.


### Direct Usage

For how to use, please see the examples:

[ConsumeExample.java](src/examples/java/io/synadia/examples/ConsumeExample.java)

[StartSequenceExample.java](src/examples/java/io/synadia/examples/StartSequenceExample.java)

[StartTimeExample.java](src/examples/java/io/synadia/examples/StartTimeExample.java)

[FetchExample.java](src/examples/java/io/synadia/examples/FetchExample.java)

[QueueConsumeExample.java](src/examples/java/io/synadia/examples/QueueConsumeExample.java)

[ErrorsExample.java](src/examples/java/io/synadia/examples/ErrorsExample.java)

The [Unit Tests](src/test/java/io/synadia/jnats/extension/DirectConsumerTests.java) may also be of interest.

---
Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
See [LICENSE](LICENSE) and [NOTICE](NOTICE) file for details.
