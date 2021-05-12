/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore

import io.xskipper.index.Index
import io.xskipper.index.execution.PartitionSpec
import io.xskipper.status.{IndexStatusResult, QueryIndexStatsResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.concurrent.Future

/**
  * a trait each [[MetadataHandle]] should implement
  * note that the tableIdentifier is not in the signature since it is likely that it will
  * be passed in the constructor.
  */
trait MetadataHandle {
  import MetadataVersionStatus._

  // hold the stats for the latest query
  protected var stats: Option[QueryIndexStatsResult] = None

  /**
    * Clears the latest stats
    */
  def clearStats(): Unit = {
      stats = None
  }

  /*
    * @return last query statistics or None if no stats are available
    */
  def getStats(): Option[QueryIndexStatsResult] = stats

  /**
    * Updates the [[MetadataHandle]] stats with the given query stats
    * called from the dataSkippingFileIndex rule in the catalyst to update the stats during
    * the query tun time
    *
    * @param currentFilterStatistics the updated stats
    */
  def updateQueryStats(currentFilterStatistics: QueryIndexStatsResult): Unit = {
    synchronized {
      stats match {
        // if stats is already defined update current stats
        case Some(curStatsInstance) =>
          curStatsInstance.update(currentFilterStatistics)
        // set the stats for the first time
        case _ => stats = Some(currentFilterStatistics)
      }
    }
  }

  /**
    * @return Maximum number of objects to index in one chunk
    */
  def getUploadChunkSize(): Int

  /**
    * @return Maximum number of objects to delete in one chunk
    */
  def getDeletionChunkSize(): Int

  /**
    * Refreshes the [[MetadataHandle]] by re-syncing with the metadatastore
    * (implementation specific)
    */
  def refresh() : Unit

  /**
    * Uploads the metadata to the metadatastore
    * This method may assume that the metadata version status
    * is [[MetadataVersionStatus.CURRENT]]
    *
    * @param metaData  RDD that contains for each file a list of abstract metaData types
    *                  to be uploaded
    * @param indexes   a sequence of indexes that created the metadata
    * @param isRefresh indicates whether the operation is a refresh operation
    */
  def uploadMetadata(metaData: RDD[Row],
                     partitionSchema: Option[StructType],
                     indexes: Seq[Index],
                     isRefresh: Boolean): Unit

  /**
    * Initialize metadata for a dataset in the metadatastore
    * (implementation specific)
    *
    * @param indexes a sequence of metadata indexes
    */
  def initMetadataUpload(indexes: Seq[Index]): Unit = {}

  /**
    * Finalize metadata creation in the metadatastore
    * (implementation specific)
    */
  def finalizeMetadataUpload(): Unit

  /**
    * Drops all of the metadata associated with the given index
    */
  def dropAllMetadata(): Unit

  /**
    * Returns a set of all indexed files (async)
    * @param filter optional filter to apply
    *        (can be used to get all indexed file for a given partition)
    * @return a set of all indexed files ids
    */
  def getAllIndexedFiles(filter: Option[Any] = None): Future[Set[String]]

  /**
    * Returns the required file ids for the given query (async)
    *
    * @param query the query to be used in order to get the relevant files
    *              (this query is of type Any and it is the responsibility of the metadatastore
    *              implementation to cast it to as instance which matches the translation for
    *              this MetaDataStore)
    * @param filter an optional filter to apply
    *        (can be used to get all indexed file for a given partition)
    * @return the set of fileids required for this query
    */
  def getRequiredObjects(query: Any, filter: Option[Any] = None): Future[Set[String]]

  /**
    * Removes the metadata for a sequence of files.
    * This method may assume that the Metadta version status
    * is [[MetadataVersionStatus.CURRENT]]
    *
    * @param files a sequence of files for which the metadata will be removed
    */
  def removeMetaDataForFiles(files: Seq[String]): Unit

  /**
    * Set metadataStore specific parameters such as user, password, host
    * Note: the implementation should not count on the Map not changing during the run
    * therefore, it needs to save its parameters locally
    *
    * @param params a map of parameters (each metadata store expects certain input)
    */
  def setParams(params: Map[String, String]): Unit

  // overload - getting a java map (for python module)
  // using final to avoid overloading by inherited class
  final def setParams(params: java.util.Map[String, String]): Unit = {
    setParams(params.asScala.toMap)
  }

  /**
    * Cleans the [[MetadataHandle]] instance
    * (implementation specific)
    */
  def clean(): Unit = {}

  /**
    * returns the version status of the metadata.
    * we do not have a strict requirement for the metadatastore to use metadata
    * from a version different than its current version for filtering / refresh
    * but we do expect it to be able to tell the version status,
    * and whether or not it can be upgraded to comply with the current version.
    *
    * @return the version of the metadata
    */
  def getMdVersionStatus(): MetadataVersionStatus

  /**
    * returns whether or not the metadata can be upgraded to
    * comply with the current version
    *
    * @return true if the metadata can be upgraded, false otherwise
    */
  def isMetadataUpgradePossible(): Boolean

  /**
    * Upgrades the metadata to comply with the current version
    * @param indexes - the indexes stored in the metadataStore.
    * @param fileIndex the file index of the indexed dataset or table
    */
  def upgradeMetadata(indexes: Seq[Index], fileIndex: FileIndex): Unit

  /**
    * returns the sequence of indexes that exist in the metadatastore for the tableIdentifier
    */
  def getIndexes(): Seq[Index]

  /**
    * Returns index statistics
    */
  def getIndexStatus(): IndexStatusResult

  /**
    * Returns true if an index exists for the tableIdentifier
    *
    * @return true if the an index exists for the tableIdentifier
    */
  def indexExists(): Boolean

  /**
    * Returns whether or not this [[MetadataHandle]] supports encryption
    *
    * @return true if the [[MetadataHandle]] supports encryption
    */
  def isEncryptionSupported(): Boolean = false
}
