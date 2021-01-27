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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType

class InMemoryDataSkippingIndex(
         sparkSession: SparkSession,
         rootPathsSpecified: Seq[Path],
         parameters: Map[String, String],
         userSpecifiedSchema: Option[StructType],
         fileStatusCache: FileStatusCache = NoopCache,
         userSpecifiedPartitionSpec: Option[PartitionSpec] = None,
         metadataOpsTimeNs: Option[Long] = None,
         tableIdentifiers: Seq[String],
         fileFilters: Seq[DataSkippingFileFilter],
         metadataFilterFactories: Seq[MetadataFilterFactory],
         clauseTranslators: Seq[ClauseTranslator],
         backend: MetadataStoreManagerType) extends InMemoryFileIndex(sparkSession,
                                                               rootPathsSpecified,
                                                               parameters,
                                                               userSpecifiedSchema,
                                                               fileStatusCache,
                                                               userSpecifiedPartitionSpec,
                                                               metadataOpsTimeNs) {

  // this is the method that will be executed by spark to check which files
  // are relevant to the query during processing time
  override def listFiles(partitionFilters: Seq[Expression],
                         dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    // first we run the original listFiles operation that includes partition pruning
    @transient lazy val prunedPartitions = super.listFiles(partitionFilters, dataFilters)
    // filter the prunedPartitions if possible
    @transient lazy val selectedPartitions = if (dataFilters.isEmpty) {
      logInfo("dataFilters is empty, avoiding unnecessary calls to isRequired")
      prunedPartitions
    } else {
      var filteredPartitions = prunedPartitions
      try {
        // Filter the partition directories by applying the filters one after the other
        // Note: previously we have applied the filters in parallel. However, since the abstraction
        // enables each filter to filter the entire sequence of partitions at once using the
        // fileFilters in parallel would mean that we have to merge their results since in the
        // typical case there will be only 1 fileFilter and since the FileFilters might need spark
        // to process the filtering (e.g Parquet metadatastore) filtering one after the other should
        // reduce the Seq[PartitionDirectory] after every call and not overwhelm spark
        fileFilters.foreach(filter => {
          filter.init(dataFilters, partitionFilters, metadataFilterFactories, clauseTranslators)
          if (filter.isSkipabble()) {
            // using to list to force evaluation since filteredPartitions is lazy evaluated
            filteredPartitions = filteredPartitions.map(partition => {
              PartitionDirectory(partition.values, partition.files
                .filter(f => filter.isRequired(f))
              )
            }).toList
          }
          // update stats in index meta
          filter.handleStatistics()
        })
        // remove all empty partitions
        filteredPartitions.filter(_.files.nonEmpty)
      }
      catch {
        // Was decided to passthrough the query in any error in the meanwhile
        case e: Throwable =>
          logInfo("Query without data skipping\n" + e.getMessage + "\n" + e.printStackTrace())
          // return the original pruned partitions
          prunedPartitions
      }
    }
    selectedPartitions
  }
}
