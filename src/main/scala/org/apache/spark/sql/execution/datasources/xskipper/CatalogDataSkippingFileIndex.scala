/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains code from the spark project (original license above).
 * It contains modifications, which are licensed as follows:
 */

/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.execution.datasources.xskipper

import io.xskipper.metadatastore.{ClauseTranslator, MetadataStoreManagerType}
import io.xskipper.search.DataSkippingFileFilter
import io.xskipper.search.filters.MetadataFilterFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, InMemoryFileIndex, PrunedInMemoryFileIndex}

/**
  * Used to preserve the capabilities of [[CatalogFileIndex]]
  */
class CatalogDataSkippingFileIndex(
                                    sparkSession: SparkSession,
                                    table: CatalogTable,
                                    tableIdentifier: String,
                                    fileFilter: DataSkippingFileFilter,
                                    sizeInBytes: Long,
                                    metadataFilterFactories: Seq[MetadataFilterFactory],
                                    clauseTranslators: Seq[ClauseTranslator],
                                    backend: MetadataStoreManagerType)
  extends CatalogFileIndex(sparkSession, table, sizeInBytes) with Logging {

  // filter the partitions using the original function behaviour
  // and wrap the result with data skipping capable index
  override def filterPartitions(filters: Seq[Expression]): InMemoryFileIndex = {
    // create the original FileIndex
    val fileIndex = super.filterPartitions(filters)
    // reconstructing FileStatusCache to avoid re listing
    val fileStatusCache = DataSkippingUtils.recreateFileStatusCache(sparkSession, fileIndex)
    val dataSkippingFileIndex = fileIndex match {
      case prunedInMemoryFileIndex: PrunedInMemoryFileIndex =>
        new PrunedInMemoryDataSkippingIndex(sparkSession,
          new Path(table.storage.locationUri.get),
          fileStatusCache, fileIndex.partitionSpec(),
          prunedInMemoryFileIndex.metadataOpsTimeNs,
          Seq(tableIdentifier),
          Seq(fileFilter),
          metadataFilterFactories,
          clauseTranslators,
          backend)
      case inMemoryFileIndex: InMemoryFileIndex =>
        new InMemoryDataSkippingIndex(sparkSession,
          rootPaths,
          table.storage.properties,
          userSpecifiedSchema = Some(inMemoryFileIndex.partitionSchema),
          fileStatusCache,
          Seq(tableIdentifier),
          Seq(fileFilter),
          metadataFilterFactories,
          clauseTranslators,
          backend)
    }
    dataSkippingFileIndex
  }
}
