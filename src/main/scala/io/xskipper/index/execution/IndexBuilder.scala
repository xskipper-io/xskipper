/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.index.execution

import io.xskipper.configuration.XskipperConf
import io.xskipper.index.{BloomFilterIndex, Index, MinMaxIndex, ValueListIndex}
import io.xskipper.status.{RefreshResult, Status}
import io.xskipper.utils.Utils
import io.xskipper.{Xskipper, XskipperException}
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import java.io.IOException
import java.util.Locale
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * Helper class for building indexes
  *
  * @param spark    [[org.apache.spark.sql.SparkSession]] object
  * @param uri      the URI of the dataset / the identifier of the
  *                 table on which the index is defined
  * @param xskipper xskipper the [[Xskipper]] instance associated with this [[IndexBuilder]]
  */
class IndexBuilder(spark: SparkSession, uri: String, xskipper: Xskipper)
  extends Logging {

  val metadataProcessor = MetadataProcessor(spark, uri, xskipper.metadataHandle)

  import spark.implicits._

  // the list of indexes the user requested
  val indexes = new ArrayBuffer[Index]()

  /**
    * Adds a MinMax index for the given column
    *
    * @param col the column to add the index on
    */
  def addMinMaxIndex(col: String): IndexBuilder = {
    indexes.append(MinMaxIndex(col))
    this
  }

  /**
    * Adds a MinMax index for the given column
    *
    * @param col         the column to add the index on
    * @param keyMetadata the key metadata to be used
    */
  def addMinMaxIndex(col: String, keyMetadata: String): IndexBuilder = {
    indexes.append(MinMaxIndex(col, Some(keyMetadata)))
    this
  }

  /**
    * Adds a ValueList index for the given column
    *
    * @param col the column to add the index on
    */
  def addValueListIndex(col: String): IndexBuilder = {
    indexes.append(ValueListIndex(col))
    this
  }

  /**
    * Adds a ValueList index for the given column
    *
    * @param col         the column to add the index on
    * @param keyMetadata the key metadata to be used
    */
  def addValueListIndex(col: String, keyMetadata: String): IndexBuilder = {
    indexes.append(ValueListIndex(col, Some(keyMetadata)))
    this
  }

  /**
    * Adds a BloomFilter index for the given column
    *
    * @param col the column to add the index on
    */
  def addBloomFilterIndex(col: String): IndexBuilder = {
    addBloomFilterIndex(col,
      XskipperConf.BLOOM_FILTER_FPP.defaultValue,
      XskipperConf.BLOOM_FILTER_NDV.defaultValue)
  }

  /**
    * Adds a BloomFilter index for the given column
    *
    * @param col         the column to add the index on
    * @param keyMetadata the key metadata to be used
    */
  def addBloomFilterIndex(col: String, keyMetadata: String): IndexBuilder = {
    addBloomFilterIndex(col, XskipperConf.BLOOM_FILTER_FPP.defaultValue, keyMetadata)
  }

  /**
    * Adds a BloomFilter index for the given column
    *
    * @param col the column to add the index on
    * @param fpp the false positive rate to use
    * @param ndv the expected number of distinct values in the bloom filter
    */
  def addBloomFilterIndex(col: String,
                          fpp: Double = XskipperConf.BLOOM_FILTER_FPP.defaultValue,
                          ndv: Long = XskipperConf.BLOOM_FILTER_NDV.defaultValue): IndexBuilder = {
    indexes.append(BloomFilterIndex(col, fpp, ndv))
    this
  }

  /**
    * Adds a BloomFilter index for the given column
    *
    * @param col         the column to add the index on
    * @param fpp         the false positive rate to use
    * @param keyMetadata the key metadata to be used
    */
  def addBloomFilterIndex(col: String,
                          fpp: Double,
                          keyMetadata: String): IndexBuilder = {
    indexes.append(BloomFilterIndex(col, fpp, keyMetadata = Some(keyMetadata)))
    this
  }

  /**
    * Adds a custoom index
    *
    * @param index the index instance to add
    */
  def addCustomIndex(index: Index): IndexBuilder = {
    indexes.append(index)
    this
  }

  /**
    * Adds a custom index
    * (Overload for python)
    *
    * @param index       the index name
    * @param cols        the sequence of columns
    * @param params      the index instance to add
    * @param keyMetadata the key metadata to be used
    */
  def addCustomIndex(index: String,
                     cols: Array[String],
                     params: java.util.Map[String, String],
                     keyMetadata: String): IndexBuilder = {
    indexes.append(Utils.createCustomIndex(index, cols, Some(keyMetadata), params.asScala.toMap))
    this
  }

  /**
    * Adds a custom index
    * (Overload for python)
    *
    * @param index  the index name
    * @param cols   the sequence of columns
    * @param params the index instance to add
    */
  def addCustomIndex(index: String,
                     cols: Array[String],
                     params: java.util.Map[String, String]): IndexBuilder = {
    indexes.append(Utils.createCustomIndex(index, cols, None, params.asScala.toMap))
    this
  }

  /**
    * Build index operation for non table URI
    *
    * @param reader a [[DataFrameReader]] instance to enable reading the URI as a
    *               [[DataFrame]]
    *               Note: The reader is assumed to have all of the parameters configured.
    *               `reader.load(Seq(<path>))` will be used by the indexing code to read each
    *               object separately
    * @return a [[DataFrame]] indicating if the operation succeeded or not
    */
  def build(reader: DataFrameReader): DataFrame = {
    // make sure the path doesn't contain any wildcards
    if (new Path(uri).toString.exists("{}[]*?\\".toSet.contains)) {
      throw new XskipperException(s"""${Status.GLOBPATH_INDEX}""")
    }
    build(reader.load(uri))
  }

  /**
    * Build index operation for table URI
    * It is assumed that the URI that was used in Xskipper definition is the
    * identifier of a table (<db>.<table>)
    *
    * @return a [[DataFrame]] indicating if the operation succeeded or not
    */
  def build(): DataFrame = {
    // get the dataframe of the table
    val df = Utils.getTable(spark, uri)
    // Indexing of hive tables is available only for partitioned hive tables
    val schemaPartitions = Utils.getPartitionColumns(df)
    if (schemaPartitions.isEmpty) {
      throw new XskipperException(s"""${Status.HIVE_NON_PARTITIONED_TABLE}""")
    }
    build(df)
  }

  /**
    * Builds the index on the given dataframe
    *
    * @param df the [[DataFrame]] of objects to be indexed - can be either a dataset created
    *           by [[SparkSession.read]] on some hadoop file system path or a
    *           hive table on top of some hadoop file system
    * @return a [[DataFrame]] indicating if the operation succeeded or not
    */
  private def build(df: DataFrame): DataFrame = {
    if (xskipper.isIndexed()) {
      throw new XskipperException(s"""${Status.INDEX_ALREADY_EXISTS}""")
    }

    try {
      createOrRefreshExistingIndex(df, indexes, false)
    } catch {
      // IO exceptions typically occur when authentication fails this can happen if the access
      // token expired (in SQL Query after 1h)
      case e: IOException =>
        logError("Index creation failed", e)
        throw new XskipperException("Index creation failed, This is most likely an " +
          "authentication failure (token expiration), please use REFRESH to continue indexing", e)
      case xskipperException: XskipperException => throw xskipperException
      case e: Exception =>
        logError("Index creation failed", e)
        throw new XskipperException("Index creation failed, use DESCRIBE to view the " +
          "current index state and REFRESH to continue indexing", e)
    }
  }

  /**
    * Validate indexes
    *
    * @param df        the [[DataFrame]] to be indexed
    * @param indexes   a sequence of [[Index]]'s to validate on the given [[DataFrame]]
    * @param schemaMap the [[DataFrame]] schema map
    * @throws XskipperException in case the requested build is invalid
    */
  private def runIndexValidations(df: DataFrame, indexes: Seq[Index],
                                  schemaMap: Map[String, (String, StructField)]): Unit = {
    // make sure there is some index to build, that df is not empty and that
    // we are not already indexed.
    if (indexes.isEmpty) {
      throw new XskipperException(s"Index creation failed: ${Status.INVALID_INDEX_TYPE}")
    }

    // verify no index duplication exists
    if (indexes.groupBy(idx => (idx.getName, idx.getCols)).size < indexes.size) {
      throw new XskipperException(Status.DUPLICATE_INDEX)
    }

    // if at least 1 index is encrypted, make sure the MetadataHandle can encrypt
    if (indexes.exists(_.isEncrypted()) && !xskipper.metadataHandle.isEncryptionSupported()) {
      throw new XskipperException(
        Status.encryptionNotSupportedError(xskipper.metadataHandle.getClass().getSimpleName))
    }

    val dataTypeMap = schemaMap.mapValues(v => (v._1, v._2.dataType))

    // Extract partition columns
    val schemaPartitions = Utils.getPartitionColumns(df)

    // make sure all requested indexing parameters are valid
    indexes.foreach(index => {
      // check if columns exist - the index returns its columns in lower case
      index.getCols.foreach(col => {
        if (!schemaMap.contains(col)) {
          throw new XskipperException(Status.nonExistentColumnError(col))
        }
        if (schemaPartitions.contains(col)) {
          throw new XskipperException(Status.partitionColumnError(col))
        }
      })
      // perform index specific validations
      index.isValid(df, dataTypeMap)
    })
  }

  /**
    * Set indexes with optimizations values according to the format and options
    *
    * @param df      a [[DataFrame]] to index created by reader format load
    * @param indexes a sequence of indexes to be applied on the [[DataFrame]]
    */
  private def updateIndexOptimizations(df: DataFrame, indexes: Seq[Index]) = {
    // no-op currently
  }

  /**
    * Creates or refresh an existing index by the [[DataFrame]] of the data to be indexed
    * (assumed to be comprised of objects)
    * This method first collects the objects that are already indexed and then indexes
    * only the non indexed objects
    *
    * @param df        the [[DataFrame]] to be indexed - can be either a dataset created by
    *                  [[SparkSession.read]] on some hadoop file system path or a hive
    *                  table on top of some hadoop file system
    * @param indexes   a sequence of [[Index]] that will be applied on the [[DataFrame]]
    * @param isRefresh whehther or not this is a refresh operation.
    *                  this is only required because in case of refresh we ignore index stats
    *                  (instead of initializing them)
    * @return a [[DataFrame]] of the format status, #indexedFiles, #removedFiles
    */
  def createOrRefreshExistingIndex(df: DataFrame,
                                   indexes: Seq[Index], isRefresh: Boolean): DataFrame = {
    // extract the format and options to enable reading of each object individually
    val (format, rawOptions) = df.queryExecution.optimizedPlan.collect {
      case l@LogicalRelation(hfs: HadoopFsRelation, _, _, _) =>
        (hfs.fileFormat.toString, hfs.options)
    }(0)

    // filter out "path" or "paths" entries from the options.
    // these options are not part of the original reader options
    // passed in, they are added because we are grabbing the relation options
    // which contain the reader options + the path/paths.
    val optionsToFilter = Set("path", "paths")
    val options = rawOptions.filterKeys(k =>
      !optionsToFilter.contains(k.toLowerCase(Locale.ROOT)))

    // Extract the dataframe schema - to avoid each index validation extracting it
    // the extraction includes the column name in lower case (for comparison with user input)
    // the column name as it appears in the schema and the datatype
    val schemaMap = Map(df.schema.flatMap(field =>
      Utils.getSchemaFields(field)): _*)

    // generate indexes cols map
    indexes.foreach(_.generateColsMap(schemaMap))

    // if this is a refresh, we run the preparation (which means upgrade)
    if (isRefresh) {
      metadataProcessor.prepareForRefresh(indexes)
    }

    // make sure the index creation request is valid
    runIndexValidations(df, indexes, schemaMap)

    // update index optimizations if possible
    updateIndexOptimizations(df, indexes)

    // Note we rely on the file index to do the right listing meaning that for hive tables
    // it will also list object in partitions which are not under the location defined in the table
    val files = MetadataProcessor.listFilesFromDf(df)
    val (newOrModifiedFilesIDs, fileIDsForRemove) =
      metadataProcessor.collectNewFiles(files, isRefresh)

    // remove unnecessary files
    var removedCount = 0
    if (fileIDsForRemove.nonEmpty) {
      removedCount = metadataProcessor.removeMetadataForFiles(fileIDsForRemove)
    }

//     for local file system, due to JDK-8177809, in some cases millis get truncated from
//     the file's last modification time. some Identifier impls rely on the last
//     modification time to generate the file id. this can cause a race condition in rare
//     cases (but it still happened in tests..) where a file is indexed and updated but
//     the file id does not change due to the millis being truncated - this can cause a file
//     to be erroneously skipped.
//     the condition that might happen is this: object A is created at time X and subsequently
//     updated at time Y, where X < Y but if we strip the millis, X==Y.
//     if we indexed the first version of A, we might wrongfully use the metadata
//     later, when the second version of A is the one that's available.
//     note that this is a rather exotic case, and it requires the indexing
//     to complete super fast (the first version has to be indexed for this to reproduce,
//     and the first version lives for a very short time), but it can still happen
//     in tests if the line below is removed.
//     if we assume the underlying storage system guarantees that at time T, all objects
//     who's last modification time < T are available (both by listing and for read)
//     then by sleeping 1001 millis between the listing and the actual indexing,
//     we ensure that the version we'll index is always the latest from all the versions
//     of that object that are equal to it when stripping the millis -
//     note that from all said versions, it's also the only that we might be asked about
//     in query time.
    Thread.sleep(1001)

    // upload new/updated files metadata
    if (newOrModifiedFilesIDs.length > 0) {
      // upload metadata
      metadataProcessor.analyzeAndUploadMetadata(
        format,
        options,
        indexes,
        newOrModifiedFilesIDs,
        Some(df.schema),
        isRefresh)
      xskipper.metadataHandle.refresh()
    }

    // return the operation values
    Seq(RefreshResult(Status.SUCCESS, newOrModifiedFilesIDs.length, removedCount)).toDS().toDF()
  }
}
