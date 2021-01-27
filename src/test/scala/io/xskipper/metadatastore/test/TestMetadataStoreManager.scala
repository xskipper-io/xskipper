/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.test

import io.xskipper.index.Index
import io.xskipper.metadatastore.MetadataVersionStatus.MetadataVersionStatus
import io.xskipper.metadatastore.{MetadataHandle, MetadataStoreManager, MetadataStoreManagerType}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.FileIndex

import scala.collection.mutable

// scalastyle:off not.implemented.error.usage
object TestMetadataStoreManager extends MetadataStoreManager with Logging {

  override def getOrCreateMetadataHandle(sparkSession: SparkSession,
                                         tableIdentifier: String): MetadataHandle = {
    new TestMetadataHandle(sparkSession, tableIdentifier)
  }

  /**
    * @return the MetaDataStoreType associated with this backend
    */
  override def getType: MetadataStoreManagerType = Test

  override def getDataSkippingFileFilterParams(
                 tid: String,
                 sparkSession: SparkSession,
                 fileIndex: FileIndex): Map[String, String] = ???

  override def init(): Unit = ???

  override def listIndexedDatasets(sparkSession: SparkSession):
  Map[String, (MetadataVersionStatus, Seq[Index])] = ???

  def getParams(sparkSession: SparkSession) : Map[String, String] = Map.empty

  override def getActiveMetadataHandles(): Map[String, MetadataHandle] = ???

  override def createMetadataHandle(sparkSession: SparkSession,
                                    tableIdentifier: String): MetadataHandle = ???
}
// scalastyle:off not.implemented.error.usage

