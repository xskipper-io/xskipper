/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.execution.datasources.xskipper

import io.xskipper.metadatastore.MetadataStoreManager
import io.xskipper.search.{DataSkippingFileFilter, DataSkippingFileFilterEvaluator}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources._

object DataSkippingUtils extends Logging {

  /**
    * Gets an inMemoryFileIndex and reconstructs the FileStatusCache
    * The way the FileStatusCache is implemented in spark makes it to not be shareable between
    * instances meaning that calling FileStatusCache.getOrCreate(spark) will result in an empty
    * cache and thus will require a new listing when the FileIndex is being replaced with a data
    * skipping FileIndex.
    * To avoid this code reconstructs the cache using the existing FileIndex and then it can be
    * used by the new FileIndex.
    * Note: the reason we can't get the FileStatusCache of the original inMemoryFileIndex is
    * because it is handed over to it in the constructor and is not defined there as var/val
    * so we can't access it once we have an instance of inMemoryFileIndex
    *
    * @param spark             a spark session - used to get a new cache
    * @param inMemoryFileIndex the inMemoryFileIndex to construct the cache from
    * @return a FileStatusCache populated with the root paths from the given inMemoryFileIndex
    */
  def recreateFileStatusCache(spark: SparkSession,
                              inMemoryFileIndex: InMemoryFileIndex): FileStatusCache = {
    // reconstructing FileStatusCache to avoid re listing
    val fileStatusCache = FileStatusCache.getOrCreate(spark)
    inMemoryFileIndex.rootPaths.foreach(path =>
      fileStatusCache.putLeafFiles(path,
        inMemoryFileIndex.listLeafFiles(Seq(path)).toArray))
    fileStatusCache
  }

  /**
    * Gets the DataSkippingFileFilter relevant for this tid, FileIndex and backend
    *
    * @param fileIndex            the fileIndex for which we create a DataSkippingFileFilter
    * @param tid                  the table identifier
    * @param metadataStoreManager the backend to be used to create the DataSkippingFileFilter
    * @param sparkSession         the spark session
    * @param evaluate             whether we create an evaluate DataSkippingFileFilter which only
    *                             report skipping stats
    * @return
    */
  def getFileFilter(fileIndex: FileIndex,
                    tid: String,
                    metadataStoreManager: MetadataStoreManager,
                    sparkSession: SparkSession,
                    evaluate: Boolean = false): DataSkippingFileFilter = {
    if (evaluate) {
      new DataSkippingFileFilterEvaluator(
        tid,
        metadataStoreManager,
        sparkSession,
        metadataStoreManager.getDataSkippingFileFilterParams(tid, sparkSession, fileIndex))

    } else {
      new DataSkippingFileFilter(tid,
        metadataStoreManager, sparkSession,
        metadataStoreManager.getDataSkippingFileFilterParams(tid, sparkSession, fileIndex))
    }
  }

  /**
    * Inject a rule as part extendedOperatorOptimizationRule
    */
  def injectRuleExtendedOperatorOptimizationRule(
                                                  sparkSession: SparkSession,
                                                  rule: Rule[LogicalPlan]): Unit = {
    // insert the rule as extendedOperatorOptimizationRule
    // Note: if this is called multiple time the rule will be injected multiple times, though it
    // won't have effect on the correctness.
    // The reason we can't remove an existing rule is because the variable optimizerRules in
    // SparkSessionExtensions is private[this].
    // Also, another option is to build the optimization rules using the buildOptimizerRules method
    // in SparkSessionExtensions and check whether the rule is already there, however,
    // this option is not good enough since we don't know if building the existing rules will have
    // any side effect.
    logInfo(s"Injecting rule ${rule.getClass.getCanonicalName}" +
      s" as part of the extended operator optimization rules")
    sparkSession.extensions.injectOptimizerRule(_ => rule)
  }
}
