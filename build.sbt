// Copyright 2021 IBM Corp.
// SPDX-License-Identifier: Apache-2.0

name := "xskipper-core"

crossScalaVersions := Seq("2.13.10")

scalaVersion := crossScalaVersions.value.head

sparkVersion := "4.0.0"

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-hive" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "provided",

  // test dependencies
  "org.scalatest" %% "scalatest" % "3.2.15" % "test",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "test",
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "test",
  "org.apache.spark" %% "spark-hive" % sparkVersion.value % "test",
  "com.googlecode.json-simple" % "json-simple" % "1.1" % "test",
  // dependency for InMemoryKMS to test parquet encryption
  "org.apache.parquet" % "parquet-hadoop" % "1.12.2" % "test"
)

// Shared list of JVM exports
val javaOpens = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

// Apply to all JVM phases
javaOptions ++= javaOpens
javaOptions in Test ++= javaOpens
javaOptions in Compile ++= javaOpens
javaOptions in run ++= javaOpens

/**
  * Test settings
  */
 // Tests cannot be run in parallel since multiple Spark contexts cannot run in the same JVM.
 parallelExecution in Test := false

fork in Test := true

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDG")

// Configurations to speed up tests and reduce memory footprint
javaOptions in Test ++= Seq(
  "-Dspark.ui.enabled=false",
  "-Dspark.ui.showConsoleProgress=false",
  "-Xmx1024m"
)

/**
  * ScalaStyle settings
  */

scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"

// Run each test in different JVM
testGrouping in Test := (definedTests in Test).value map { test =>
  Tests.Group(name = test.name, tests = Seq(test), runPolicy = Tests.SubProcess(
    ForkOptions(
      javaHome.value,
      outputStrategy.value,
      Nil,
      Some(baseDirectory.value),
      javaOptions.value,
      connectInput.value,
      envVars.value
    )))
}

// Run as part of compile task
lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := scalastyle.in(Compile).toTask("").value
(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value

// Run as part of test task
lazy val testScalastyle = taskKey[Unit]("testScalastyle")
testScalastyle := scalastyle.in(Test).toTask("").value
(test in Test) := ((test in Test) dependsOn testScalastyle).value


/**
  * Spark Packages settings
  */

spName := "xskipper-io/xskipper-core"

spAppendScalaVersion := true

spIncludeMaven := true

spIgnoreProvided := true

packageBin in Compile := spPackage.value

/*
 * Doc settings
 */

scalacOptions in (Compile, doc) ++= Seq(
  "-no-link-warnings" // Suppresses problems with Scaladoc @throws links
)

/**
  * Release settings
  */
organization := "io.xskipper"
organizationName := "xskipper"
organizationHomepage := Some(url("https://github.com/xskipper-io"))
description := "Xskipper: An Indexing Subsystem for Apache Spark"
licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
homepage := Some(url("https://github.com/xskipper-io/xskipper"))

publishTo := {
  if (isSnapshot.value) {
    Some("snapshots" at "https://central.sonatype.com/repository/maven-snapshots/")
  } else {
    Some("releases" at
      "https://ossrh-staging-api.central.sonatype.com/" +
        "service/local/staging/deploy/maven2/")
  }
}
credentials += Credentials(
  "Sonatype Nexus Repository Manager",
  "central.sonatype.com",
  sys.env("NEXUS_USER"),
  sys.env("NEXUS_PW")
)

credentials += Credentials(
  "Sonatype Nexus Repository Manager",
  "ossrh-staging-api.central.sonatype.com",
  sys.env("NEXUS_USER"),
  sys.env("NEXUS_PW")
)

releasePublishArtifactsAction := PgpKeys.publishSigned.value

publishMavenStyle := true
releaseCrossBuild := true

scmInfo := Some(
  ScmInfo(
    url("https://github.com/xskipper-io/xskipper"),
    "scm:git@github.com:xskipper-io/xskipper.git"
  )
)

developers := List(
  Developer(
    id = "guykhazma",
    name = "Guy Khazma",
    email = "",
    url = url("https://github.com/guykhazma")
  ),
  Developer(
    id = "gallushi",
    name = "Gal Lushi",
    email = "",
    url = url("https://github.com/gallushi")
  ),
  Developer(
    id = "oshritf",
    name = "Oshrit Feder",
    email = "",
    url = url("https://github.com/oshritf")
  ),
  Developer(
    id = "paulata",
    name = "Paula Ta-Shma",
    email = "",
    url = url("https://github.com/paulata")
  ),
  Developer(
    id = "ymoatti",
    name = "Yosef Moatti",
    email = "",
    url = url("https://github.com/ymoatti")
  )
)

import ReleaseTransformations._

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion
)