/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet.versioning

import io.xskipper.metadatastore.parquet.versioning.util.VersionTestUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrameReader, SparkSession}

case class VersionTest(name: String,
                       inputFormat: String,
                       dataSetOrigPath: String,
                       dataSetDestPathPrefix: String,
                       mdLocationPrefix: String,
                       targetMdLocationPrefix: String,
                       minVersion: Long,
                       maxVersion: Long,
                       testId: Long,
                       query: String,
                       indexTypes: Seq[(String, Seq[String])],
                       expectedSkippedFiles: Seq[String],
                       expectedSkippedFilesAfterUpdate: Seq[String]) {
  def getFullDestPath(): String = {
    new Path(dataSetDestPathPrefix + "/" + dataSetOrigPath).toString
  }

  def getMetadataBasePath(version: Long): String = {
    s"""${System.getProperty("user.dir")}
       |/${VersionTestUtils.INPUT_REL_PATH}
       |/${mdLocationPrefix}
       |/version=${version}
       |/testID=${testId}/""".stripMargin.replaceAll("\n", "")
  }

  def getTargetMetadataBasePath(version: Long): String = {
    s"""${targetMdLocationPrefix}/version=${version}/testID=${testId}"""
  }

  def getDFReader(spark: SparkSession): DataFrameReader = {
    inputFormat match {
      case "csv" => spark.read.option("header", "true").option("inferSchema", "true").format("csv")
      case "json" => spark.read.option("inferSchema", "true").format("json")
      case "parquet" => spark.read.format("parquet")
    }
  }
}
