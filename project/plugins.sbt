// Copyright 2021 IBM Corp.
// SPDX-License-Identifier: Apache-2.0

resolvers += "spark-packages" at "https://repos.spark-packages.org"

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.0")

addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.6")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "2.1.1")