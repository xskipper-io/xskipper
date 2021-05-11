/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.test

import io.xskipper.index.Index
import io.xskipper.index.execution.PartitionSpec
import io.xskipper.metadatastore.MetadataVersionStatus.MetadataVersionStatus
import io.xskipper.metadatastore.{MetadataHandle, MetadataStoreManagerType, MetadataVersionStatus}
import io.xskipper.status.IndexStatusResult
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable
import scala.concurrent.Future

case object Test extends MetadataStoreManagerType

// scalastyle:off not.implemented.error.usage
class TestMetadataHandle(val session: SparkSession, tableIdentifier: String)
  extends MetadataHandle with Logging with Serializable {
  /** Maximum number of objects to index in one chunk */
  override def getUploadChunkSize(): Int = Integer.MAX_VALUE

  /** Maximum number of objects to delete in one chunk */
  override def getDeletionChunkSize(): Int = ???

  override def uploadMetadata(metaData: RDD[Row],
                              partitionSpec: Option[StructType],
                              indexes: Seq[Index],
                              isRefresh: Boolean): Unit = {}

  /**
    * Initialize metadata for a dataset in the metadatastore
    *
    * @param indexes a sequence of metadata indexes
    */
  override def initMetadataUpload(indexes: Seq[Index]): Unit = {}

  /**
    * Finalize dataset's metadata creation in the metadatastore
    */
  override def finalizeMetadataUpload(): Unit = {}

  /**
    * Drops all of the metadata associated with the given index
    */
  override def dropAllMetadata(): Unit = ???

  /**
    * Returns a set of all indexed files (async)
    * @param filter optional filter to apply
    *        (can be used to get all indexed file for a given partition)
    * @return a set of all indexed files ids
    */
  def getAllIndexedFiles(filter: Option[Any] = None): Future[Set[String]] = ???

  /**
    * Removes the metadata for a sequence of files
    *
    * @param files a sequence of files for which the metadata will be remove
    */
  override def removeMetaDataForFiles(files: Seq[String]): Unit = ???

  /**
    * Set metadataStore specific parameters such as user, password, host
    * Note: the implementation should not count on the Map not changing during the run
    * therefore, it needs to save its parameters locally
    *
    * @param params a map of parameters (each metadata store expects certain input)
    */
  override def setParams(params: Map[String, String]): Unit = ???

  /**
    * Cleans the metadata store instance (implementation specific)
    */
  override def clean(): Unit = ???

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
  override def getRequiredObjects(query: Any, filter: Option[Any] = None): Future[Set[String]] = ???

  override def getMdVersionStatus(): MetadataVersionStatus = {
    MetadataVersionStatus.CURRENT
  }

  override def isMetadataUpgradePossible(): Boolean = true

  override def upgradeMetadata(indexes: Seq[Index], fileIndex: FileIndex): Unit = {}

  /**
    * returns the a sequence of indexes that exist in the metadata store for the tableIdentifier
    * be passed in the constructor.
    */
  override def getIndexes(): Seq[Index] = ???

  /**
    * Returns index statistics
    */
  override def getIndexStatus(): IndexStatusResult = ???

  /**
    * Refreshes indexMeta by rereading the stats from the metadata store
    */
  override def refresh(): Unit = {}

  /**
    * Returns true if an index exists for the tableIdentifier
    *
    * @return
    */
  override def indexExists(): Boolean = false
}
// scalastyle:on not.implemented.error.usage
