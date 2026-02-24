<img src="../orbit_shorter.png" alt="Orbit">

# Batch Publish

Utility to publish an atomic batch, a group of up to 1000 messages

### Important

* Messages are stored in memory on the server until the commit.
* Batch currently is not about speed, it's about transaction, meaning all the messages must be added to the stream or none of them do.

https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-50.md

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:batch--publish-197556?labelColor=grey&style=flat)
![0.2.2](https://img.shields.io/badge/Current_Release-0.2.2-27AAE0)
![0.2.3](https://img.shields.io/badge/Current_Snapshot-0.2.3--SNAPSHOT-27AAE0)
[![Dependencies Help](https://img.shields.io/badge/Dependencies%20Help-27AAE0)](https://github.com/synadia-io/orbit.java?tab=readme-ov-file#dependencies)
[![javadoc](https://javadoc.io/badge2/io.synadia/batch-publish/javadoc.svg)](https://javadoc.io/doc/io.synadia/batch-publish)
[![Maven Central](https://img.shields.io/maven-central/v/io.synadia/batch-publish)](https://img.shields.io/maven-central/v/io.synadia/batch-publish)


---
Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
See [LICENSE](LICENSE) and [NOTICE](NOTICE) file for details.
