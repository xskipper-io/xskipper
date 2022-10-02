<!--
 -- Copyright 2021 IBM Corp.
 -- SPDX-License-Identifier: Apache-2.0
 -->

![Xskipper](site/docs/img/logo_readme.png)

![Build Status](https://github.com/xskipper-io/xskipper/workflows/build/badge.svg)

Xskipper is an Extensible Data Skipping Framework for [Apache Spark](https://spark.apache.org/).

To get started, see the [Quick Start Guide](https://xskipper.io/getting-started/quick-start-guide/) .

See Xskipper [site](https://xskipper.io) for more info. 

# Run as a project

To build a project using the Xskipper binaries from the Maven Central Repository, use the following Maven coordinates:

## Maven

Include Xskipper in a Maven project by adding it as a dependency in the project's POM file. Xskipper should be compiled with Scala 2.12.

```XML
<dependency>
  <groupId>io.xskipper</groupId>
  <artifactId>xskipper-core_2.12</artifactId>
  <version>1.3.0</version>
</dependency>
```

## SBT
Include Xskipper in an SBT project by adding the following line to its build.sbt file:

```Scala
libraryDependencies += "io.xskipper" %% "xskipper-core" % "1.3.0"
```

# Building

Xskipper is compiled using [SBT](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html).

To compile, run

    build/sbt compile

To generate artifacts, run

    build/sbt package

To execute tests, run

    build/sbt test

Refer to [SBT docs](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html) for more commands.

# Collaboration

Xskipper tracks issues in GitHub and prefers to receive contributions as pull requests.

# Compatibility

Xskipper is compatible with Spark according to the following table:

| Xskipper version | Spark Version        |
| --------------- | --------------------- |
| 1.4.x           | 3.3.x   |
| 1.3.x           | 3.2.x   |
| 1.2.x           | 3.0.x   | 
| 1.1.x           | 2.4.x   | 
| 1.0.x           | 2.3.x   |

# See Also

- [IEEE Big Data 2020 paper - Extensible Data Skipping](https://arxiv.org/abs/2009.08150) (arxiv version)

# License
Apache License 2.0, see [LICENSE](LICENSE).

# Acknowledgements

This software has been developed under the [BigDataStack project](https://bigdatastack.eu/the-bigdatastack-solution), as part of the holistic solution for big data applications and operations. 
BigDataStack has received funding from the European Union’s Horizon 2020 research and innovation programme under grant agreement No 779747.
