/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.execution.datasources.v2.xskipper

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.v2.orc.OrcTable
import org.apache.spark.sql.execution.datasources.xskipper.DataSkippingUtils
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitioningAwareFileIndex}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap


class OrcDataSkippingTable(name: String,
                           sparkSession: SparkSession, options: CaseInsensitiveStringMap,
                           paths: Seq[String], userSpecifiedSchema: Option[StructType],
                           fallbackFileFormat: Class[_ <: FileFormat]) extends
  OrcTable(name, sparkSession, options, paths, userSpecifiedSchema, fallbackFileFormat) {
  override lazy val fileIndex: PartitioningAwareFileIndex =
    DataSkippingUtils.getFileIndex(sparkSession, options, paths, userSpecifiedSchema)
}
