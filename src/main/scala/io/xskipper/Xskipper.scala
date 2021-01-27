/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper

import java.text.NumberFormat

import io.xskipper.configuration.XskipperConf
import io.xskipper.implicits._
import io.xskipper.index.Index
import io.xskipper.index.execution.{IndexBuilder, MetadataProcessor}
import io.xskipper.metadatastore.MetadataVersionStatus._
import io.xskipper.metadatastore._
import io.xskipper.metadatastore.parquet.ParquetMetadataStoreManager
import io.xskipper.status.{Status, StatusResult}
import io.xskipper.utils.Utils
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object Xskipper {
  /**
    * Updates JVM wide xskipper parameters (Only given parameters will be updated)
    *
    * @param params a map of parameters to be set
    */
  def setConf(params: Map[String, String]): Unit = {
    XskipperConf.setConf(params)
  }

  // overload - getting a java map (for python module)
  def setConf(params: java.util.Map[String, String]): Unit = {
    XskipperConf.setConf(params)
  }

  /**
    * Returns a map of all configurations currently set
    */
  def getConf(): java.util.Map[String, String] = XskipperConf.getConf()

  /**
    * Sets a specific key in the JVM wide configuration
    *
    * @param key the key to set
    * @param value the value associated with the key
    */
  def set(key: String, value: String): Unit = {
    XskipperConf.set(key, value)
  }

  /**
    * Removes a key from the configuration
    *
    * @param key the key to remove
    */
  def unset(key: String): Unit = {
    XskipperConf.unset(key)
  }

  /**
    * Retrieves the value associated with the given key in the configuration
    *
    * @param key the key to lookup
    * @return the value associated with the key or null if the key doesn't exist
    *         (null is returned so this function can be used in the python module)
    */
  def get(key: String): String = XskipperConf.get(key)

  /**
    * Gets the aggregated latest query skipping stats for all active [[MetadataHandle]] instances
    * in the current default [[MetadataStoreManager]].
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
    * In case the API was called on a query which didn't involve any index or the API was called
    * without running a query the returned [[DataFrame]] structure is -
    * status, reason with status=FAILED
    * In case the query cannot be skipped because one of the following:
    * 1. No dataset in the query has no indexed files
    * 2. No query to the metadata store can be generated -
    *    can be due to a predicate that can not be used in skipping
    *    (or due to missing metadata filter)
    *    or due to failure to translate the abstract query.
    * the returned [[DataFrame]] structure is:
    * status, isSkippable, skipped_Bytes, skipped_Objs, total_Bytes, total_Objs with status=SUCCESS,
    *         isSkippable=false and all other values are -1
    * Otherwise the [[DataFrame]] structure is the same as above with isSkippable=true
    * and the relevant stats
    *
    * @param sparkSession a spark session to construct the dataframe with the latest query stats
    * @return a [[DataFrame]] object containing information about latest
    *         query stats
    */
  def getLatestQueryAggregatedStats(sparkSession: SparkSession) : DataFrame = {
    import sparkSession.implicits._
    val metadataStoreManager = Registration.getActiveMetadataStoreManager()
    if (metadataStoreManager.getActiveMetadataHandles().nonEmpty) {
      return Seq(metadataStoreManager.getLatestQueryAggregatedStats()).toDS().toDF()
    }
    // the API was called on a query which didn't involve any index or the API was called without
    // running a query the returned dataframe structure is - status, reason with status=FAILED
    Seq(StatusResult(Status.FAILED, Status.NO_DATA)).toDS().toDF()
  }

  /**
    * Clears the stats for all active [[MetadataHandle]] instances
    * in the active [[MetadataStoreManager]]
    * Should be called before each query to make sure the aggregated
    * stats are cleared
    */
  def clearStats() : Unit = {
    val metadataStoreManager = Registration.getActiveMetadataStoreManager()
    metadataStoreManager.clearStats()
  }

  /**
    * Reset all xskipper settings by:
    * 1. disables filtering
    * 2. clear all [[MetadataHandle]] in the default [[MetadataStoreManager]]
    * 3. reset the JVM wide configuration
    * @param sparkSession the spark session to remove the rule from
    */
  def reset(sparkSession: SparkSession): Unit = {
    sparkSession.disableXskipper()
    val metadataStoreManager = Registration.getActiveMetadataStoreManager()
    metadataStoreManager.clearActiveMetadataHandles()
    Registration.reset()
    XskipperConf.clearConf()
  }

  /**
    * Returns information about the indexed datasets
    *
    * @param sparkSession
    * @return a [[DataFrame]] object containing information about the
    *         indexed datasets under the configured base path
    */
  def listIndexes(sparkSession: SparkSession): DataFrame = {
    // Get metadatastore used
    val metdataStoreManager = Registration.getActiveMetadataStoreManager()

    val indexes: Map[String, (MetadataVersionStatus, Seq[Index])] =
      metdataStoreManager.listIndexedDatasets(sparkSession)
    val res: ArrayBuffer[Row] = ArrayBuffer.empty[Row]
    res += Row("# Metadatastore Manager parameters", "", "")
    metdataStoreManager.getParams(sparkSession).foreach{case (key, value) =>
      res += Row(key, value, "")}
    indexes.foreach {
      case (identifier: String, (verStat: MetadataVersionStatus, indexes: Seq[Index])) =>
      var firstRow: Boolean = true
      // print index information -  dataset name will appear once, at the first dataset's index
      indexes.foreach(index => {
        val displayName = firstRow match {
          case true if verStat == DEPRECATED_SUPPORTED || verStat == DEPRECATED_UNSUPPORTED =>
            s"${verStat.toString.toUpperCase()} ${Utils.getTableIdentifierDisplayName(identifier)}"
          case true => Utils.getTableIdentifierDisplayName(identifier)
          case false => ""
        }
        res += Row(displayName, index.getName, index.getCols.mkString(","))
        firstRow = false
      })
    }

    val indexSchema = StructType(Array(
      StructField("Dataset", StringType, true),
      StructField("Index type", StringType, true),
      StructField("Index columns", StringType, true)))
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(res, 1), indexSchema)
  }

  /**
    * Python API Wrapper for enabling Xskipper in the given [[SparkSession]]
    *
    * @param sparkSession [[SparkSession]] object
    */
  def enable(sparkSession: SparkSession): Unit = sparkSession.enableXskipper()

  /**
    * Python API Wrapper for disabling Xskipper in the given [[SparkSession]]
    *
    * @param sparkSession [[SparkSession]] object
    */
  def disable(sparkSession: SparkSession): Unit = sparkSession.disableXskipper()

  /**
    * Python API Wrapper for checking if Xskipper is enabled
    *
    * @param sparkSession [[SparkSession]] object
    * @return true if the Xskipper is enabled for the current [[SparkSession]]
    */
  def isEnabled(sparkSession: SparkSession): Boolean = sparkSession.isXskipperEnabled()
}


/**
  * Main class for programmatically interacting with Xskipper
  *
  * @param sparkSession sparkSession instance for processing
  * @param uri the URI of the dataset / the identifier of the table
  *            on which the index is defined
  * @param metadataStoreManager The [[MetadataStoreManager]] to use
  */
class Xskipper(sparkSession: SparkSession, val uri: String,
               metadataStoreManager: MetadataStoreManager = ParquetMetadataStoreManager) {
  /**
    * Additional constructor for pySpark API
    *
    * @param sparkSession sparkSession instance for processing
    * @param uri the URI of the dataset / the identifier of the table
    *            on which the index is defined
    * @param metadataStoreManagerClassName fully qualified name of [[MetadataStoreManager]]
    *                                      to be used
    * @throws XskipperException if the metadataStoreManagerClassName is invalid
    */
  def this(sparkSession: SparkSession, uri: String, metadataStoreManagerClassName: String) {
    this(sparkSession, uri,
      Utils.getObjectInstance[MetadataStoreManager](metadataStoreManagerClassName) match {
        case Some(mdBackend) => mdBackend
        case None => throw new XskipperException(s"""Could not create Xskipper,
           ${metadataStoreManagerClassName} is not a valid MetadataStoreManager class name""")})
  }

  import sparkSession.implicits._

  val tableIdentifier = Utils.getTableIdentifier(uri)
  // the metadataHandle associated with the instance - using a function to make
  // sure using only active instances
  private[xskipper] def metadataHandle() =
    metadataStoreManager.getOrCreateMetadataHandle(sparkSession,
    tableIdentifier)

  /**
    * Deletes the index
    *
    * @throws XskipperException if index cannot be removed
    */
  def dropIndex() : Unit = {
    if (isIndexed()) {
      metadataHandle.dropAllMetadata()
      metadataHandle.refresh()
      if (isIndexed()) throw new XskipperException(Status.FAILED_REMOVE)
    }
  }

  /**
    * Refresh index operation for non table URI
    *
    * @param reader a [[DataFrameReader]] instance to enable reading the URI as a
    *               [[DataFrame]]
    *               Note: The reader is assumed to have all of the parameters configured.
    *               `reader.load(Seq(<path>))` will be used by the indexing code to read each
    *               object separately
    * @return [[DataFrame]] object containing statistics about
    *         the refresh operation
    * @throws XskipperException if index cannot be refreshed
    */
  def refreshIndex(reader: DataFrameReader): DataFrame = {
    refreshIndex(reader.load(uri))
  }

  /**
    * Refresh index operation for table URI
    *
    * @return [[DataFrame]] object containing statistics about
    *         the refresh operation
    * @throws XskipperException if index cannot be refreshed
    */
  def refreshIndex() : DataFrame = {
    val df = Utils.getTable(sparkSession, uri)
    refreshIndex(df)
  }

  /**
    * Refresh the index according to the data in the given [[DataFrame]]
    *
    * @param df the [[DataFrame]] to be indexed - can be either a dataset
    *           that was created by `spark.read`` on some hadoop file system path or a table on
    *           top of some hadoop file system
    * @return [[DataFrame]] object containing statistics about the
    *         refresh operation
    * @throws XskipperException if index cannot be refreshed
    */
  private def refreshIndex(df: DataFrame): DataFrame = {
    if (!isIndexed()) throw new XskipperException(Status.INDEX_NOT_FOUND)
    // getting the indexes
    val indexes = metadataHandle.getIndexes()
    val res = indexBuilder().createOrRefreshExistingIndex(df, indexes, true)
    res
  }

  /**
    * Checks if the URI is indexed
    *
    * @return true if the URI is indexed
    */
  def isIndexed(): Boolean = {
    metadataHandle.indexExists()
  }

  /**
    * Helper class for setting and building an index
    */
  def indexBuilder() : IndexBuilder = new IndexBuilder(sparkSession, uri, this)

  /**
    * Describes the indexes on the URI (for non table URI)
    *
    * @param reader a [[DataFrameReader]] instance to enable reading the URI as a
    *               [[DataFrame]]
    * @return [[DataFrame]] object containing information about the index
    * @throws XskipperException if the URI is not indexed
    */
  def describeIndex(reader : DataFrameReader) : DataFrame = {
    generateDescribeIndex(reader.load(uri))
  }

  /**
    * Describes the indexes on the URI (for table URI)
    *
    * @return [[DataFrame]] object containing information about the index
    * @throws XskipperException if the URI is not indexed
    */
  def describeIndex() : DataFrame = {
    val df = Utils.getTable(sparkSession, uri)
    generateDescribeIndex(df)
  }

  /**
    * return meta index info like indexing scheme and skipping stats
    */
  protected def generateDescribeIndex(df: DataFrame): DataFrame = {
    if (!isIndexed()) throw new
        XskipperException(Status.INDEX_NOT_FOUND)

    val res: ArrayBuffer[Row] = ArrayBuffer.empty[Row]

    // get the index stats
    val indexStats = metadataHandle.getIndexStatus()
    val files = MetadataProcessor.listFilesFromDf(df)
    val metadataProcessor = MetadataProcessor(sparkSession, uri, metadataHandle)
    val (newOrModifiedFilesIDs, filesToRemove) = metadataProcessor.collectNewFiles(files, true)
    val numNewModifiedFiles = newOrModifiedFilesIDs.length
    val numFilesToRemove = filesToRemove.length
    val numCurrentFiles = files.length
    val percentNew = NumberFormat.getPercentInstance.format(
      numNewModifiedFiles.toDouble / numCurrentFiles)

    if (numNewModifiedFiles > 0) {
      res += Row("Status", "Out of date - please use REFRESH operation to update the index", "")
      res += Row("Total new/modified objects", s"""${percentNew} of the objects
         (${numNewModifiedFiles} of ${numCurrentFiles})""", "")
    } else if (numFilesToRemove > 0) {
      res += Row("Status", s"""Index is up to date but contains ${numFilesToRemove} obsolete
         objects - you can use REFRESH operation to remove these objects""", "")
    }
    else {
      res += Row("Status", "Up to date", "")
    }
    res += Row("Total objects indexed", indexStats.numberOfIndexedObjects.toString, "")
    // print metadatastore specific information
    if (indexStats.metadataVersionStatus == DEPRECATED_SUPPORTED ||
      indexStats.metadataVersionStatus == DEPRECATED_UNSUPPORTED) {
      res += Row("# Metadata version status",
        indexStats.metadataVersionStatus.toString.toUpperCase(), "")
    }
    if (indexStats.metadataStoreSpecificProperties.size > 0) {
      res += Row("# Metadata properties", "", "")
      indexStats.metadataStoreSpecificProperties.foreach{case(key, value) =>
        res += Row(key, value, "")}
    }
    // print index information
    res += Row("# Index information", "", "")
    res += Row("# Index type", "Columns", "Params")

    indexStats.indexes.foreach(descriptor => res +=
      Row(descriptor.getName, descriptor.getCols.mkString(","),
        descriptor.getParams.map{case (key, value) =>
          s"${key} -> ${value}"}.mkString(" ; "), ""))

    val tableIdentifierFormmatted = Utils.getTableIdentifierDisplayName(tableIdentifier)

    val indexStatsSchema = StructType(Array(
      StructField("Data Skipping Index Stats", StringType, true),
      StructField(tableIdentifierFormmatted, StringType, true),
      StructField("Comment", StringType, true)))
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(res, 1), indexStatsSchema)
  }

  /**
    * Return latest query skipping statistics for this Xskipper instance
    *
    * In case the API was called on a URI without an index or the API was called without
    * running a query the returned [[DataFrame]] structure is -
    * status, reason with status=FAILED
    * In case the query cannot be skipped because one of the following:
    * 1. Dataset has no indexed files
    * 2. No query to the metadata store can be generated - can be due to a predicate that can not
    *    be used in skipping (or maybe due to missing metadata filter) or due to failure to
    *    translate the abstract query.
    *    the returned dataframe structure is:
    *       status, isSkippable, skipped_Bytes, skipped_Objs, total_Bytes,
    *       total_Objs with status=SUCCESS, isSkippable=false and all other values are -1
    *       Otherwise the [[DataFrame]] structure is the same as above with isSkippable=true
    *       and the relevant stats
    *
    * @return [[DataFrame]] object containing information about latest
    *         query stats
    */
  def getLatestQueryStats() : DataFrame = {
    metadataHandle.getStats() match {
      case Some(stats) =>
        Seq(stats).toDS().toDF()
      case None =>
        // Not an error - empty stat could be generated instead of failed status if desired
        Seq(StatusResult(Status.FAILED, Status.NO_DATA)).toDS().toDF()
    }
  }

  /**
    * Update instance specific [[MetadataHandle]] parameters
    */
  def setParams(params: Map[String, String]): Unit = {
    metadataHandle.setParams(params)
  }

  // overload - getting a java map (for python module)
  def setParams(params: java.util.Map[String, String]) : Unit = {
    setParams(params.asScala.toMap)
  }
}
