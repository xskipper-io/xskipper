/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper

import org.apache.spark.sql.SparkSession

trait XskipperProvider {
  val datasourceV2: Boolean
  lazy val spark: SparkSession = {
    var builder = SparkSession.builder()
      .appName("Xskipper Tests")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.xskipper.utils.RuleExtension")
    if (datasourceV2) {
      builder =
        builder.config("spark.sql.sources.useV1SourceList", "avro,kafka,text")
    }
    builder.enableHiveSupport().getOrCreate()
  }

  def getXskipper(uri: String): Xskipper
}
