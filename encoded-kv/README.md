<img src="../orbit_shorter.png" alt="Orbit">

# Encoded Key Value

Encoded Key Value provides a way to use Key Value with encoded keys and values.

It is a Java Generic version of the Key Value interface and provides the ability
* to have something other than a string for a key.
* to have an object instead of a byte array as a value

It requires a _codec_, which 
* encodes the key object as a string
* encodes the value object as a byte array
* decodes the encoded key back to the key object
* decodes the encoded data bytes back into the value object.

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:encoded--kv-197556?labelColor=grey&style=flat)
![0.0.4](https://img.shields.io/badge/Current_Release-0.0.4-27AAE0)
![0.0.5](https://img.shields.io/badge/Current_Snapshot-0.0.5--SNAPSHOT-27AAE0)
[![Dependencies Help](https://img.shields.io/badge/Dependencies%20Help-27AAE0)](https://github.com/synadia-io/orbit.java?tab=readme-ov-file#dependencies)
[![javadoc](https://javadoc.io/badge2/io.synadia/encoded-kv/javadoc.svg)](https://javadoc.io/doc/io.synadia/encoded-kv)
[![Maven Central](https://img.shields.io/maven-central/v/io.synadia/encoded-kv)](https://img.shields.io/maven-central/v/io.synadia/encoded-kv)

---
Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
See [LICENSE](LICENSE) and [NOTICE](NOTICE) file for details.
