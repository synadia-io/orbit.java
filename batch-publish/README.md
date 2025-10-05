![Synadia](src/main/javadoc/images/synadia-logo.png) &nbsp;&nbsp;&nbsp;&nbsp; ![NATS](src/main/javadoc/images/large-logo.png)

# Batch Publish

Utility to publish an atomic batch, a group of up to 1000 messages

### Important

* Messages are stored in memory on the server until the commit.
* Batch currently is not about speed, it's about transaction, meaning all the messages must be added to the stream or none of them do.

https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-50.md

**Current Release**: 0.2.1
&nbsp;**Current Snapshot**: 0.2.2-SNAPSHOT
&nbsp; **Gradle and Maven** `io.synadia:batch-publish`
[Dependencies Help](https://github.com/synadia-io/orbit.java?tab=readme-ov-file#dependencies) 

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:batch--publish-00BC8E?labelColor=grey&style=flat)
[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/batch-publish/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/batch-publish)
[![javadoc](https://javadoc.io/badge2/io.synadia/batch-publish/javadoc.svg)](https://javadoc.io/doc/io.synadia/batch-publish)


---
Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
See [LICENSE](LICENSE) and [NOTICE](NOTICE) file for details.
