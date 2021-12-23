/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore

import java.util.concurrent.ConcurrentHashMap

import io.xskipper.configuration.XskipperConf
import io.xskipper.index.Index
import io.xskipper.metadatastore.MetadataVersionStatus.MetadataVersionStatus
import io.xskipper.status.{QueryIndexStatsResult, Status}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.FileIndex

import scala.collection.JavaConverters._

// Abstract class that all MetadataStoreManager types have to extend in order to be defined
abstract class MetadataStoreManagerType

/**
  * A trait each [[MetadataStoreManager]] should implement
  */
trait MetadataStoreManager {
  /**
    * @return the [[MetadataStoreManagerType]] associated with this [[MetadataStoreManager]]
    */
  def getType: MetadataStoreManagerType


  protected var objectStatsEnabled: Boolean = true

  // holds all of the active Metadata in the JVM
  val metadataHandlesInstanceMap: ConcurrentHashMap[String, MetadataHandle] =
    new ConcurrentHashMap[String, MetadataHandle]()

  /**
    * Gets an existing [[MetadataHandle]] or, if there is no existing one, creates a new
    * one.
    *
    * @param sparkSession    the sparks session
    * @param tableIdentifier the table identifier of the dataset for which the [[MetadataHandle]]
    *                        instance is created
    * @return a new instance of the [[MetadataHandle]] associated with this backend
    */
  def getOrCreateMetadataHandle(sparkSession: SparkSession,
                                tableIdentifier: String): MetadataHandle = {
    val createMetadataHandlefFunc = new java.util.function.Function[String, MetadataHandle]() {
      override def apply(t: String): MetadataHandle = {
        val newInst = createMetadataHandle(sparkSession, tableIdentifier)
        // set relevant parameters for new instances only using JVM wide parameters
        newInst.setParams(XskipperConf.confEntries)
        newInst
      }
    }
    metadataHandlesInstanceMap.computeIfAbsent(tableIdentifier, createMetadataHandlefFunc)
  }

  /**
    * Creates a new instance [[MetadataHandle]]
    * Used by [[getOrCreateMetadataHandle]] in order to create new instances
    *
    * @param sparkSession
    * @param tableIdentifier
    * @return
    */
  def createMetadataHandle(sparkSession: SparkSession,
                           tableIdentifier: String): MetadataHandle

  /**
    * Returns all active [[MetadataHandle]]s managed by this [[MetadataStoreManager]]
    */
  def getActiveMetadataHandles(): Map[String, MetadataHandle] =
    metadataHandlesInstanceMap.asScala.toMap

  /**
    * Clear all active [[MetadataHandle]]s managed by this [[MetadataStoreManager]]
    */
  def clearActiveMetadataHandles(): Unit = {
    metadataHandlesInstanceMap.values().iterator().asScala.foreach(_.clean())
    metadataHandlesInstanceMap.clear()
    enableSkippedObjectStats()
  }

  /**
    * Returns a map of parameters to be set by the [[DataSkippingFileFilter]]
    * on the [[MetadataHandle]] when using this [[MetadataStoreManager]].
    * This map can be used to pass specific parameters depending on the type of FileIndex that is
    * being replaced.
    * For example - in [[parquet.ParquetMetadataStoreManager]] we can
    * specify that the metadata location should be inferred from the table/default db properties
    *
    * @param tid          the table identifier for which the DataSkippingFileFilter will be created
    * @param sparkSession the sparks session
    * @param fileIndex    the fileIndex that will be replaced by
    *                     [[DataSkippingFileFilter]]
    * @return a map of parameters to be set on the MetadataStore when the DataSkippingFileFilter
    *         is created
    */
  def getDataSkippingFileFilterParams(
                                       tid: String,
                                       sparkSession: SparkSession,
                                       fileIndex: FileIndex): Map[String, String]

  /**
    * A unit function which enables to do setup for the [[MetadataStoreManager]].
    * The setup is called once when the [[MetadataStoreManager]] is registered
    */
  def init(): Unit

  /**
    * @return Map of indexed datasets to their respective md version status and indexes
    */
  def listIndexedDatasets(sparkSession: SparkSession):
  Map[String, (MetadataVersionStatus, Seq[Index])]

  /**
    * @return a map representing relevant [[MetadataStoreManager]] parameters
    */
  def getParams(sparkSession: SparkSession): Map[String, String]

  /**
    * Gets the aggregated latest query skipping stats for all active [[MetadataHandle]] instances.
    * In order to get reliable results it is assumed that either [[clearStats]]
    * or [[clearActiveMetadataHandles]] was called before running the query.
    *
    * This is needed since the way we aggregate the skipping stats is by going over all active
    * [[MetadataHandle]]s of the [[MetadataStoreManager]] and aggregating their stats.
    * When running multiple queries there could be a scenario in which the first query used
    * dataset `a` and the second query didn't use it, therefore, when calling aggregatedStats
    * for the second query the [[MetadataHandle]] for dataset `a` will be present
    * as an active [[MetadataHandle]] therefore we need its stats to be cleared.
    *
    * @return [[QueryIndexStatsResult]] instance with the latest query aggregated stats
    */
  def getLatestQueryAggregatedStats(): QueryIndexStatsResult = {
    val aggregatedStats: QueryIndexStatsResult =
      QueryIndexStatsResult(Status.SUCCESS, false, 0, 0, 0, 0)
    // get all active metadata handles
    // aggregate the index stats
    getActiveMetadataHandles().foreach {
      case (_: String, metadataHandle: MetadataHandle) =>
        val currStats = metadataHandle.getStats()
        // update stats only if relevant,  None stats can exist for dataset which are not relevant
        // for the latest query (have been set to None either by clearStats or cleanContext)
        currStats match {
          case Some(curStatsInstance) =>
            aggregatedStats.update(curStatsInstance)
          case _ =>
        }
    }
    // if skipped object stats are disabled, set all numeric fields to 0
    if (!skippedObjectStatsEnabled) {
      QueryIndexStatsResult(aggregatedStats.status,
        aggregatedStats.isSkippable,
        0, 0, 0, 0)
    }
    aggregatedStats
  }

  /**
    * Disables the Skipped Object stats, other fields are still maintained.
    */
  def disableSkippedObjectStats(): Unit = {
    objectStatsEnabled = false
  }

  /**
    * Enables the skipped Objects stats
    */
  def enableSkippedObjectStats(): Unit = {
    objectStatsEnabled = true
  }

  /**
    * whether the Skipped Object stats are enabled
    * @return
    */
  def skippedObjectStatsEnabled: Boolean = objectStatsEnabled

  /**
    * Clears the stats for all active [[MetadataHandle]] instances.
    * Should be called before each query (or call [[clearActiveMetadataHandles]])
    * to make sure the aggregated stats are cleared
    */
  def clearStats(): Unit = {
    metadataHandlesInstanceMap.values().asScala.foreach {
      case (metaDataHandle: MetadataHandle) => metaDataHandle.clearStats()
    }
    enableSkippedObjectStats()
  }
}
