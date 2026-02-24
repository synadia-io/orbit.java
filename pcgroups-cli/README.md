<img src="../orbit_shorter.png" alt="Orbit">

# Partitioned Consumer Groups CLI

The Partitioned Consumer Groups CLI is a command line tool. In the usage,

[![0.1.0](https://img.shields.io/badge/Current_Release-0.1.0-27AAE0)](https://github.com/synadia-io/orbit.java/releases/tag/pcgcli%2F0.1.0)

#### Downloads

| tar                                                                                        | zip                                                                                        |
|--------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------|
| [cg.tar](https://github.com/synadia-io/orbit.java/releases/download/pcgcli%2F0.1.0/cg.tar) | [cg.zip](https://github.com/synadia-io/orbit.java/releases/download/pcgcli%2F0.1.0/cg.zip) |

## Usage

`cg` stands for `java -jar <path>/cg.jar` 

```
Usage: cg <command> [options]

Commands:
  static   Static consumer groups mode
  elastic  Elastic consumer groups mode

Use 'cg <command> --help' for more information about a command.
```

```
Usage: cg static [COMMAND]
Static consumer groups mode
Commands:
  ls, list                        List static consumer groups for a stream
  info                            Get static consumer group info
  create                          Create a static partitioned consumer group
  delete, rm                      Delete a static partitioned consumer group
  member-info, memberinfo, minfo  Get static consumer group member info
  step-down, stepdown, sd         Initiate a step down for a member
  consume, join                   Join a static partitioned consumer group
  prompt                          Interactive prompt mode
```

```
Usage: cg elastic [COMMAND]
Elastic consumer groups mode
Commands:
  ls, list                           List elastic consumer groups for a stream
  info                               Get elastic consumer group info
  create                             Create an elastic partitioned consumer
                                       group
  delete, rm                         Delete an elastic partitioned consumer
                                       group
  add                                Add members to an elastic consumer group
  drop                               Drop members from an elastic consumer group
  create-mapping, cm, createmapping  Create member mappings for an elastic
                                       consumer group
  delete-mapping, dm, deletemapping  Delete member mappings for an elastic
                                       consumer group
  member-info, memberinfo, minfo     Get elastic consumer group member info
  step-down, stepdown, sd            Initiate a step down for a member
  consume, join                      Join an elastic partitioned consumer group
  prompt                             Interactive prompt mode
```

## Building from Source
The project contains both a Maven pom.xml file and a Gradle project,
and both are configured to build an executable Java jar named `cg.jar`

### Maven
```
mvn clean package
``` 

will build the `cg.jar` in the `target` folder 

### Gradle
```
gradle clean package
```
will build the `cg.jar` in the `build` folder

## Running


```
java -jar target/cg.jar ...
java -jar build/cg.jar ...
```

---
Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
See [LICENSE](LICENSE) and [NOTICE](NOTICE) file for details.
