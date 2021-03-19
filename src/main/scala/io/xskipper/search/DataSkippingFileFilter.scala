/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.search

import io.xskipper.configuration.XskipperConf
import io.xskipper.metadatastore.{ClauseTranslator, MetadataStoreManager, TranslationUtils}
import io.xskipper.search.filters.MetadataFilterFactory
import io.xskipper.status.{QueryIndexStatsResult, Status}
import io.xskipper.utils.Utils
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Expression}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * A Custom FileFilter which enables to filter sequence of PartitionDirectory using
  * a given MetadataBackend
  *
  * @param tid the table identifier for which the [[DataSkippingFileFilter]] is built
  * @param metadataStoreManager the [[MetadataStoreManager]] to be used in order to get
  *                             [[io.xskipper.metadatastore.MetadataHandle]] for the identifier
  * @param params a map of parameters to be set by the [[DataSkippingFileFilter]]
  *               on the [[io.xskipper.metadatastore.MetadataHandle]]
  */
class DataSkippingFileFilter(tid: String,
                             metadataStoreManager: MetadataStoreManager,
                             sparkSession: SparkSession,
                             params: Map[String, String] =
                              Map.empty[String, String]) extends Logging {
  // init to the default value from the JVM conf
  // once an instance is created the configuration is modified if needed
  private val TIMEOUT = XskipperConf.getConf(XskipperConf.XSKIPPER_TIMEOUT)

  // saves the filter statistics, note that in case of an InMemoryFileIndex with multiple root
  // paths (which can result when using hive tables) then it will have multiple filteredPartitions
  // which are evaluated lazily since this is a stream therefore the stats are accumulated
  protected val currentFilterStatistics = mutable.HashMap[String, Long](
    "REQUIRED_OBJ_COUNT" -> 0,
    "REQUIRED_BYTES_SUM" -> 0,
    "TOTAL_OBJ_COUNT" -> 0,
    "TOTAL_BYTES_SUM" -> 0)
  protected lazy val metadataHandler =
    metadataStoreManager.getOrCreateMetadataHandle(sparkSession, tid)

  // indicates whether the current query is relevant for skipping.
  // i.e - it has indexed files and a metadata query can be generated
  private var isSkippable = false

  private var required: Set[String] = Set.empty
  private var indexed: Set[String] = Set.empty

  /**
    * Filters the partition directory by removing unnecessary objects from each partition directory
    *
    * @param dataFilters query predicates for actual data columns (not partitions)
    * @param partitionFilters the partition predicates from the query
    * @param metadataFilterFactories a sequence of MetadataFilterFactory to generate filters
    *                                according to the index on the dataset
    * @param clauseTranslators a sequence of ClauseTranslators to be applied on the clauses
    * @return a sequence of PartitionDirectory after filtering the unnecessary objects
    *         using the metadata
    */
  def init(
            dataFilters: Seq[Expression],
            partitionFilters: Seq[Expression],
            metadataFilterFactories: Seq[MetadataFilterFactory],
            clauseTranslators: Seq[ClauseTranslator]): Unit = {
    // set the metadata store params - needed when we infer the metadata location from hive table/db
    metadataHandler.setParams(params)
    // get the indexes
    val indexDescriptor = metadataHandler.getIndexes().distinct

    // strip the qualifier from the attribute reference as we have the same
    // attribute in the metadata
    val partitionExp = partitionFilters.isEmpty match {
      case false =>
        val res = partitionFilters.reduce(And).transform {
          case attr: AttributeReference => attr.withQualifier(Seq.empty)
        }
        Some(res)
      case true => None
    }

    if (!indexDescriptor.isEmpty) {
      // get the abstract query
      val filters = metadataFilterFactories.map(_.getFilters(indexDescriptor)).flatten.distinct
      val abstractFilterQuery = MetadataQueryBuilder.getClause(dataFilters, filters)

      abstractFilterQuery match {
        case Some(abstractQuery) =>
          // translate the query to the metadatastore representation
          val translatedQuery = TranslationUtils.getClauseTranslation[Any](
            metadataStoreManager.getType, abstractQuery, clauseTranslators)
          translatedQuery match {
            case Some(queryInstance) =>
              isSkippable = true // translation succeeded so dataset is skippable
              logInfo(s"Filtering partitions using " +
                s"${metadataStoreManager.getType.toString} backend")
              logInfo("Getting all indexed files and required files")
              val indexedFut = metadataHandler.getAllIndexedFiles(partitionExp)
              val requiredFut = metadataHandler.getRequiredObjects(queryInstance,
                partitionExp)
              indexed = Await.result(indexedFut, TIMEOUT minutes)
              required = Await.result(requiredFut, TIMEOUT minutes)
              if (log.isTraceEnabled()) {
                (indexed -- required).foreach(f => logTrace(s"""${f}--->SKIPPABLE!"""))
              }
            case _ =>
              logInfo("No translation for the abstract query => no skipping")
          }
        case _ =>
          logInfo("No abstract query generated => no skipping")
      }
    } else {
      logInfo("Dataset is not indexed => no skipping")
    }
  }

  /**
    * Returns true if the current file is required for the given query by checking
    * if it is present in the required files or not indexed
    *
    * @param fs the file status to check
    * @return true if the file is required, false otherwise
    */
  def isRequired(fs: FileStatus): Boolean = {
    val id = Utils.getFileId(fs)
    // the file isRequired if it is in the required files or not indexed
    val res = required.contains(id) || !indexed.contains(id)
    updateStats(fs, res)
    res
  }

  /**
    * Update the stats for a given fileStatus
    *
    * @param fs the fileStatus to be updated
    * @param shouldScan whether this file should be scanned or not
    */
  protected def updateStats(fs: FileStatus, shouldScan: Boolean): Unit = {
    // wrapped in if to avoid calculating the id in non debug mode
    if (log.isDebugEnabled) {
      log.debug(s"${Utils.getFileId(fs)} ${if (shouldScan) "" else "--------> SKIPPED!"}")
    }
    if (shouldScan) {
      currentFilterStatistics("REQUIRED_OBJ_COUNT") += 1
      currentFilterStatistics("REQUIRED_BYTES_SUM") += fs.getLen
    }
    currentFilterStatistics("TOTAL_OBJ_COUNT") += 1
    currentFilterStatistics("TOTAL_BYTES_SUM") += fs.getLen
  }

  /**
    * Clears the current filter statistics.
    * Should be called at the end each call to handleStatistics which updates the accumulated stats
    */
  protected def clearCurrentFilterStatistics() : Unit = {
    currentFilterStatistics("TOTAL_OBJ_COUNT") = 0
    currentFilterStatistics("TOTAL_BYTES_SUM") = 0
    currentFilterStatistics("REQUIRED_OBJ_COUNT") = 0
    currentFilterStatistics("REQUIRED_BYTES_SUM") = 0
  }

  /**
    * @return true if the current query is relevant for skipping.
    *         i.e - it has indexed files and a metadata query can be generated
    */
  def isSkipabble(): Boolean = {
    isSkippable
  }

  /**
    * Update the IndexMeta associated with the table
    *
    */
  def handleStatistics(): Unit = {
    // if the query cannot be skipped because no query to the metadata store can be generated
    // can be due to a predicate that can not be used in skipping
    // (or maybe due to missing metadata filter)
    // or due to failure to translate the abstract query.
    // updated stats to indicate that the index was not used
    if (!isSkippable) {
      // update query stats to reflect non skippable query
      metadataHandler.updateQueryStats(QueryIndexStatsResult(
        Status.SUCCESS,
        false,
        0,
        0,
        0,
        0))
    }
    // if a query to the metadata store was generated and there are some indexed files update
    // the stats accordingly
    else {
      logInfo(s"File Filter Selected ${currentFilterStatistics.get("REQUIRED_OBJ_COUNT").get}" +
        s" files out of ${currentFilterStatistics.get("TOTAL_OBJ_COUNT").get}")
      currentFilterStatistics.put("SKIPPED_OBJ_COUNT",
        currentFilterStatistics.get("TOTAL_OBJ_COUNT").get -
          currentFilterStatistics.get("REQUIRED_OBJ_COUNT").get)
      currentFilterStatistics.put("SKIPPED_BYTES_SUM",
        currentFilterStatistics.get("TOTAL_BYTES_SUM").get -
          currentFilterStatistics.get("REQUIRED_BYTES_SUM").get)
      // update latest query stats reflect skippable query
      metadataHandler.updateQueryStats(QueryIndexStatsResult(
        Status.SUCCESS,
        true,
        currentFilterStatistics.get("SKIPPED_BYTES_SUM").get,
        currentFilterStatistics.get("SKIPPED_OBJ_COUNT").get,
        currentFilterStatistics.get("TOTAL_BYTES_SUM").get,
        currentFilterStatistics.get("TOTAL_OBJ_COUNT").get))
    }

    // clear current filter statistics so next run will start with empty stats
    clearCurrentFilterStatistics()
    // reset the file filter state
    resetState()
  }

  private def resetState(): Unit = {
    // clear collected data
    indexed = Set.empty
    required = Set.empty
    // set isSkippable to false
    isSkippable = false
  }
}
