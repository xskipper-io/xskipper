/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.execution.datasources.xskipper

import io.xskipper.metadatastore.{ClauseTranslator, MetadataStoreManagerType}
import io.xskipper.search.DataSkippingFileFilter
import io.xskipper.search.filters.MetadataFilterFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{FileStatusCache, PartitionSpec}

/**
  * used to support PrunedInMemoryFileIndex which might result from running
  * PruneFileSourcePartitions rule, since the PrunedInMemoryFileIndex specify its own partitionSpec
  * we can not use an InMemoryFileIndex
  */
class PrunedInMemoryDataSkippingIndex(
                                       sparkSession: SparkSession,
                                       tableBasePath: Path,
                                       fileStatusCache: FileStatusCache,
                                       override val partitionSpec: PartitionSpec,
                                       override val metadataOpsTimeNs: Option[Long],
                                       tableIdentifiers: Seq[String],
                                       fileFilters: Seq[DataSkippingFileFilter],
                                       metadataFilterFactories: Seq[MetadataFilterFactory],
                                       clauseTranslators: Seq[ClauseTranslator],
                                       backend: MetadataStoreManagerType)
  extends InMemoryDataSkippingIndex(
    sparkSession,
    partitionSpec.partitions.map(_.path),
    Map.empty,
    Some(partitionSpec.partitionColumns),
    fileStatusCache,
    tableIdentifiers,
    fileFilters,
    metadataFilterFactories,
    clauseTranslators,
    backend)
