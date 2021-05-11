/*
 * Copyright 2021 IBM Corp.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.xskipper.metadatastore.parquet

import java.io.IOException
import io.xskipper.Registration
import io.xskipper.configuration.ConfigurationUtils
import io.xskipper.index.metadata.MetadataType
import io.xskipper.index.{Index, IndexFactory, IndexFactoryUtils}
import io.xskipper.metadatastore.MetadataVersionStatus.{MetadataVersionStatus, _}
import io.xskipper.metadatastore.parquet.ParquetUtils._
import io.xskipper.metadatastore.{parquet, _}
import io.xskipper.status.IndexStatusResult
import io.xskipper.utils.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.sql.execution.datasources.parquet.SparkToParquetSchemaConverter
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.xskipper.utils.MetadataUtils

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.math.max

class ParquetMetadataHandle(val session: SparkSession, tableIdentifier: String)
  extends MetadataHandle with Logging with Serializable {

  private lazy implicit val vanillaConverter = new SparkToParquetSchemaConverter(new SQLConf())

  // Configuration parameters
  private var MD_PATH: Option[ParquetMetadataPath] = None
  private var CHUNK_SIZE = ParquetMetadataStoreConf.PARQUET_INDEX_CHUNK_SIZE.defaultValue
  private var EXPECTED_MAX_METADATA_BYTES_SIZE =
    ParquetMetadataStoreConf.PARQUET_EXPECTED_MAX_METADATA_BYTES_SIZE.defaultValue
  private var MAX_RECORDS_PER_METADATA_FILE =
    ParquetMetadataStoreConf.PARQUET_MAX_RECORDS_PER_METADATA_FILE.defaultValue
  private var DEDUP_ON_REFRESH =
    ParquetMetadataStoreConf.DEDUP_ON_REFRESH.defaultValue
  private var DEDUP_ON_FILTER =
    ParquetMetadataStoreConf.DEDUP_ON_FILTER.defaultValue
  private var FOOTER_KEY: Option[String] = None
  private var PLAINTEXT_FOOTER_ENABLED: Boolean =
    ParquetMetadataStoreConf.PARQUET_PLAINTEXT_FOOTER_ENABLED.defaultValue

  // cache to store the indexes - to avoid reading the metadata df multiple times
  private var indexesCache: Option[Seq[Index]] = Option.empty

  def setMDPath(md: String, `type`: String): ParquetMetadataHandle = {
    logDebug(s"setting metadata location to $md with type ${`type`}")
    val mdPath = new ParquetMetadataPath(session, tableIdentifier, md, `type`)
    try {
      val fs = mdPath.path.getFileSystem(session.sparkContext.hadoopConfiguration)
      fs.exists(mdPath.path)
    } catch {
      case e: Exception =>
        logInfo(s"Failure setting mdpath to ${mdPath.path}")
        throw ParquetMetaDataStoreException(
          s"Error accessing mdpath ${mdPath.path}, got ${e}")
    }
    MD_PATH = Some(mdPath)
    this
  }

  override def setParams(params: Map[String, String]): Unit = {
    // update only given parameters
    // only parameters which are relevant for the parquet metadatastore are considered
    // (meaning they start with `io.xskipper.parquet.`
    params.foreach {
      case (key: String, value: String)
        if key.startsWith(ParquetMetadataStoreConf.PARQUET_METADATASTORE_CONF_PREFIX) =>
        key match {
          case ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY => // handled below
          case ParquetMetadataStoreConf.PARQUET_MD_LOCATION_TYPE_KEY => // handled below
          case ParquetMetadataStoreConf.PARQUET_INDEX_CHUNK_SIZE_KEY =>
            logDebug(s"Set ${ParquetMetadataStoreConf.PARQUET_INDEX_CHUNK_SIZE_KEY} to ${value}")
            CHUNK_SIZE = ConfigurationUtils.getConf(
              ParquetMetadataStoreConf.PARQUET_INDEX_CHUNK_SIZE, params)
          case ParquetMetadataStoreConf.PARQUET_EXPECTED_MAX_METADATA_BYTES_SIZE_KEY =>
            // scalastyle:off line.size.limit
            logDebug(s"Set ${ParquetMetadataStoreConf.PARQUET_EXPECTED_MAX_METADATA_BYTES_SIZE_KEY} to ${value}")
            // scalastyle:on line.size.limit
            EXPECTED_MAX_METADATA_BYTES_SIZE = ConfigurationUtils.getConf(
              ParquetMetadataStoreConf.PARQUET_EXPECTED_MAX_METADATA_BYTES_SIZE, params)
          case ParquetMetadataStoreConf.PARQUET_MAX_RECORDS_PER_METADATA_FILE_KEY =>
            // scalastyle:off line.size.limit
            logDebug(s"Set ${ParquetMetadataStoreConf.PARQUET_MAX_RECORDS_PER_METADATA_FILE_KEY} to ${value}")
            // scalastyle:on line.size.limit
            MAX_RECORDS_PER_METADATA_FILE = ConfigurationUtils.getConf(
              ParquetMetadataStoreConf.PARQUET_MAX_RECORDS_PER_METADATA_FILE, params)
          case ParquetMetadataStoreConf.PARQUET_FOOTER_KEY_PARAM_KEY =>
            // do not log the label of the footer key since someone
            // may accidentally place the actual key here
            logInfo("Setting Footer key ")
            FOOTER_KEY = Some(value.toString)
            logInfo("Verifying PME Classes exist")
            // we have encryption configured. make sure PME is loaded
            // note that if the user fails to set the footer key and still
            // tries to configure encrypted indexes, another exception will be raised
            // so in any case we will never try to enable encryption without
            // PME Loaded. note we only check it here, and in particular, not checking
            // during refresh - this is since if PME isn't loaded refresh will fail since
            // we won't even be able to read the entire md (at least 1 index will be encrypted)
            if (!isPmeAvailable()) {
              throw new ParquetMetaDataStoreException(
                "Metadata encryption is specified but PME is not loaded!")
            }
          case ParquetMetadataStoreConf.PARQUET_PLAINTEXT_FOOTER_ENABLED_KEY =>
            logInfo(s"Setting plaintext footer to ${value}")
            PLAINTEXT_FOOTER_ENABLED = ConfigurationUtils.getConf(
              ParquetMetadataStoreConf.PARQUET_PLAINTEXT_FOOTER_ENABLED, params)
          case ParquetMetadataStoreConf.DEDUP_ON_REFRESH_KEY =>
            logInfo(s"Setting dedup on refresh to ${value}")
            DEDUP_ON_REFRESH = ConfigurationUtils.getConf(
              ParquetMetadataStoreConf.DEDUP_ON_REFRESH, params)
          case ParquetMetadataStoreConf.DEDUP_ON_FILTER_KEY =>
            logInfo(s"Setting dedup on filter to ${value}")
            DEDUP_ON_FILTER = ConfigurationUtils.getConf(
              ParquetMetadataStoreConf.DEDUP_ON_FILTER, params)
          case _ =>
            logWarning(s"Unknown parameter ${key} with value ${value}")
        }
      case _ =>
    }
    // Update the metadata location
    if (params.contains(ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY) &&
      params.contains(ParquetMetadataStoreConf.PARQUET_MD_LOCATION_TYPE_KEY)) {
      // infer from parameters
      logDebug(s"Set metadata location to " +
        s"${params.get(ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY).get} " +
        s"with type ${params.get(ParquetMetadataStoreConf.PARQUET_MD_LOCATION_TYPE_KEY).get}")
      val metaDataLocation: String = ConfigurationUtils.getConf(
        ParquetMetadataStoreConf.PARQUET_MD_LOCATION, params)
      val metaDataLocationType: String = ConfigurationUtils.getConf(
        ParquetMetadataStoreConf.PARQUET_MD_LOCATION_TYPE, params)
      setMDPath(metaDataLocation, metaDataLocationType)
    }
  }

  private[parquet] def loadIndexMetadata(schema: StructType, mdPath: String,
                                         indexFactories: Seq[IndexFactory])
  : Option[(String, (MetadataVersionStatus, Seq[Index]))] = {
    var indexes: ArrayBuffer[Index] = new ArrayBuffer[Index]()

    val tableIdentifier = ParquetUtils.getTableIdentifier(schema) match {
      case Some(tid) => tid
      case None =>
        logWarning(s"Metadata file ${mdPath} is corrupted. tableIdentifier not found")
        return None // Not a metadata obj, skip
    }

    val version = ParquetUtils.getVersion(schema)
    val versionStatus = ParquetUtils.getMdVersionStatus(version)

    // read the indexes from the metadata dataframe
    schema.foreach(field => if (field.metadata.contains("index")) {
      val indexMeta = field.metadata.getMetadata("index")
      // name and index are must for every index
      val indexType = indexMeta.getString("name")
      val indexCols = indexMeta.getStringArray("cols")
      var params = Map.empty[String, String]
      // adding the parameters
      if (indexMeta.contains("params")) {
        version match {
          case x if x < 0 =>
            throw new ParquetMetaDataStoreException(s"version $version cannot be negative")
          case x if x <= 2 =>
            indexMeta.getStringArray("params").foreach(s => {
              // split to build the parameter map
              val a = s.splitAt(s.indexOf(":"))
              params += (a._1 -> a._2.substring(1))
            })
          case x if x == 3L || x == 4L =>
            val paramsMetadata = indexMeta.getMetadata("params")
            MetadataUtils.getKeys(paramsMetadata).foreach(key => {
              params += (key -> paramsMetadata.getString(key))
            })
          case _ =>
            // if this code is reached then a semantic case is missing!
            // it means we don't have a path that handles this specific version.
            // this can happen when bumping the version and not changing this function
            throw ParquetMetaDataStoreException(s"no index recreation path exists for $version")
        }
      }

      // key metadata
      val keyMetadata = if (indexMeta.contains("key_metadata")) {
        Some(indexMeta.getString("key_metadata"))
      } else {
        None
      }
      // try loading the index using the given factories
      IndexFactoryUtils.getIndex(indexType, indexCols, params, keyMetadata, indexFactories) match {
        case Some(index) => indexes.append(index)
        case _ => logWarning(s"Unable to load the index ${indexType} " +
          s"with cols ${indexCols.mkString(",")} params ${params.mkString(",")} " +
          s"and keyMetadata ${keyMetadata}")
      }
    })
    Some(tableIdentifier, (versionStatus, indexes))
  }

  private def getIndexes(indexFactories: Seq[IndexFactory]): Seq[Index] = {
    if (!indexExists()) {
      return Seq.empty[Index]
    }
    // extract the indexes from the metadata
    loadIndexMetadata(getMetaDataDFRaw().schema, getMDPath.path.toString, indexFactories) match {
      case Some((_: String, (versionStatus: MetadataVersionStatus, indexes))) if
      versionStatus == CURRENT || versionStatus == DEPRECATED_SUPPORTED => indexes
      case Some((_: String, (TOO_NEW, _))) =>
        val msg = s"Metadata for ${tableIdentifier} is from a version that is too new!"
        logWarning(msg)
        throw ParquetMetaDataStoreException(msg)
      case Some((_: String, _)) =>
        val msg = s"Metadata for ${tableIdentifier} is deprecated and unsupported," +
          s" if REFRESH does not fix the problem, drop it and re-index"
        logWarning(msg)
        throw parquet.ParquetMetaDataStoreException(msg)
      case _ => throw ParquetMetaDataStoreException("Dataset index exists, no indexes metadata")
    }
  }

  def indexExists(): Boolean = {
    try {
      val mdPath = getMDPath.path
      val fs = mdPath.getFileSystem(session.sparkContext.hadoopConfiguration)
      val res = fs.exists(mdPath)
      res
    } catch {
      case e: IOException =>
        logInfo("Index was not found - " + e)
        false
      case _: ParquetMetaDataStoreException =>
        logWarning("Unable to resolve metadata path")
        false
    }
  }

  /**
    * @return Maximum number of objects to index in one chunk
    */
  override def getUploadChunkSize(): Int = CHUNK_SIZE

  /**
    * @return Maximum number of objects to delete in one chunk
    */
  override def getDeletionChunkSize(): Int = Integer.MAX_VALUE

  /**
    * Finalize metadata creation in the metadatastore
    * (implementation specific)
    */
  override def finalizeMetadataUpload(): Unit = {
    logInfo("In finalizeMetadataStore Parquet dataset metadata...")
    logInfo(s"Compacting the metadata -" +
      s"aiming for metadata size of ${EXPECTED_MAX_METADATA_BYTES_SIZE}")
    // calculate the number of partitions
    // get a listing of the old files
    val mdPath = getMDPath.path
    val fs = mdPath.getFileSystem(session.sparkContext.hadoopConfiguration)
    val totalSize = fs.listStatus(mdPath).foldLeft(0L)((s, f) => s + f.getLen)
    // avoid having less than 1 partition
    val numPartitions = max(1, (totalSize / EXPECTED_MAX_METADATA_BYTES_SIZE).toInt)
    compact(numPartitions)

    // if the index was built on a hive table add the relevant parameter to the table properties
    getMDPath.`type` match {
      case ParquetMetadataStoreConf.PARQUET_MD_LOCATION_HIVE_TABLE_NAME =>
        logInfo("Set the metadata location in the table parameters")
        val mdPath = getMDPath.path.toString
        session.sql(
          s"ALTER TABLE ${tableIdentifier} SET TBLPROPERTIES " +
            s"('${ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY}'='${mdPath}')")
        session.sql(
          s"ALTER TABLE ${tableIdentifier} UNSET TBLPROPERTIES IF EXISTS " +
            s"('${ParquetMetadataStoreConf.LEGACY_PARQUET_MD_LOCATION_KEY}')")
        // refresh the table metadata
        session.catalog.refreshTable(tableIdentifier)
      case _ =>
    }
  }

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
  override def uploadMetadata(metaData: RDD[Row],
                              partitionSchema: Option[StructType],
                               indexes: Seq[Index],
                               isRefresh: Boolean): Unit = {

    // Translate the metadata if possible to avoid serialization
    val translators = Registration.getCurrentMetaDataTranslators().collect {
      case t: ParquetMetaDataTranslator => t
    }

    val numPartitionColumns = partitionSchema match {
      case Some(spec) => spec.length
      case _ => 0
    }
    val translatedRDD = metaData.map(row => {
      val translatedMetaData: Seq[Any] = (1 + numPartitionColumns to
        indexes.size + numPartitionColumns ).map(i =>
        TranslationUtils.getMetaDataTypeTranslation(Parquet, row.getAs[MetadataType](i),
          indexes(i - 1 - numPartitionColumns), translators).getOrElse(row.getAs[Any](i)))
      val res = Row((0 until numPartitionColumns + 1).map(row.get(_)) ++ translatedMetaData: _*)
      res
    })

    // if this is a refresh and at least 1 index is encrypted then we are dealing with encrypted md
    // note that the md is encrypted iff at least 1 index is encrypted

    val schema = isRefresh match {
      // on refresh - extract existing schema - we must use it since it contains
      // encryption stuff that we don't have configured when we refresh
      // for example, footer key and plaintext footer definition.
      case true =>
        val mdDf = getMetaDataDFRaw()
        if (ParquetUtils.getMdVersionStatusFromDf(mdDf) != CURRENT) {
          // we should never get here. if the metadata is not current, this should
          // be picked up by the MetadataProcessor, and either fixed or the refresh
          // should be stopped right there.
          throw ParquetMetaDataStoreException("uploadMetadata with REFRESH was" +
            " called on non-current metadata!")
        }
        mdDf.schema
      // we are creating new md
      case false =>
        createDFSchema(indexes, partitionSchema, true,
          tableIdentifier, FOOTER_KEY, PLAINTEXT_FOOTER_ENABLED)
    }

    logInfo("Uploading metadata...")
    // Here we don't need to translate the metadata types since
    // we have registered UDT per each MetaDataType
    val df = session.createDataFrame(translatedRDD, schema)
    val writeOpts = getDFWriterOptions(df)

    // write the metadata
    df.repartition(1)
      .write.mode(SaveMode.Append)
      .options(writeOpts) // will be empty if no encrypted indexes and no footer key provided
      .parquet(getMDPath.path.toString)
  }

  /**
    * Returns a set of all indexed files (async)
    * @param filter optional filter to apply
    *        (can be used to get all indexed file for a given partition)
    * @return a set of all indexed files ids
    */
  override def getAllIndexedFiles(filter: Option[Any]): Future[Set[String]] = Future {
    var df = getRectifiedMetadataDf().select("obj_name")
    filter match {
      case Some(f) if isPartitionDataAvailable() =>
        val filterExpr = f.asInstanceOf[Expression]
        val mapping: Map[String, String] = filterExpr.references.map(ref =>
          ref.name->getPartitionColName(ref.name)).toMap
        val transformedPartitionFilter = ParquetUtils.replaceReferences(filterExpr, mapping)
        logDebug(s"Original partition filter ${filterExpr.sql}")
        logDebug(s"Transformed partition filter ${transformedPartitionFilter.sql}")
        df = df.where(transformedPartitionFilter.sql)
      case _ =>
    }
    if (DEDUP_ON_FILTER) {
      logInfo("dropping index duplicates")
      df = df.dropDuplicates("obj_name")
    }
    // Load metadata and return all name column values
    df.collect().map(_.getString(0)).toSet
  }

  /**
    * Removes the metadata for a sequence of files.
    * This method may assume that the Metadta version status
    * is [[MetadataVersionStatus.CURRENT]]
    *
    * @param files a sequence of files for which the metadata will be removed
    */
  override def removeMetaDataForFiles(files: Seq[String]): Unit = {
    // filter should not cause shuffle
    // note we are upgrading the metadata here!!!!
    replaceMetaData(getMetaDataDFRaw().filter(!col("obj_name").isin(files: _*)))
  }

  /**
    * Replaces the metadata with the given df
    * First writes the new df in append mode and then deletes the old files
    *
    * @param df the df to write
    */
  private def replaceMetaData(df: DataFrame): Unit = {
    // get a listing of the old files
    val mdPath = getMDPath.path
    val fs = mdPath.getFileSystem(session.sparkContext.hadoopConfiguration)
    val oldFiles = fs.listStatus(mdPath)
    // maxRecords sets the number of files (does not affect the number of partitions)
    // TODO: when SPARK-31988 is solved we can use save on the df
    val writeDF = session.createDataFrame(df.rdd, df.schema)
    val writeOpts = getDFWriterOptions(df)
    writeDF.write
      .option("maxRecordsPerFile", MAX_RECORDS_PER_METADATA_FILE)
      .options(writeOpts)
      .mode(SaveMode.Append)
      .parquet(getMDPath.path.toString)
    // delete old files
    oldFiles.foreach(f => fs.delete(f.getPath, true))
  }

  /**
    * Compacts the metadata by rewriting it with the given num of partitions
    * Also potentially remove duplicated rows in the metadata which might have been introduced
    * due to failed refresh operations
    *
    * @param numPartitions the number of partitions to be compacted to
    */
  private def compact(numPartitions: Int = 1): Unit = {
    logInfo("compact with #partitions : " + numPartitions)
    // read the df - use repartition and write back
    // we can work on the raw DF since by now the physical metadata must
    // be of the same version
    val df = DEDUP_ON_REFRESH match {
      case true =>
        logInfo("dropping index duplicates")
        getMetaDataDFRaw().dropDuplicates("obj_name")
      case _ => getMetaDataDFRaw()
    }
    replaceMetaData(df.repartition(numPartitions))
  }

  /**
    * Gets the number of indexed objects
    *
    * @return the number of indexed objects
    */
  def getNumberOfIndexedObjects(): Long = {
    // can work on the raw DF since it's only counting
    getMetaDataDFRaw().count()
  }

  /**
    * Drops all of the metadata associated with the given index
    */
  override def dropAllMetadata(): Unit = {
    // Delete md objects from the file system
    if (indexExists()) {
      val mdPath = getMDPath.path
      mdPath.getFileSystem(session.sparkContext.hadoopConfiguration).delete(mdPath, true)
    }

    // if the index was built on a hive table remove the relevant parameter
    // from the table properties
    getMDPath.`type` match {
      case ParquetMetadataStoreConf.PARQUET_MD_LOCATION_HIVE_TABLE_NAME =>
        logInfo("Remove the metadata location from the table parameters")
        session.sql(
          s"ALTER TABLE ${tableIdentifier} UNSET TBLPROPERTIES IF EXISTS " +
            s"('${ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY}')")
        session.sql(
          s"ALTER TABLE ${tableIdentifier} UNSET TBLPROPERTIES IF EXISTS " +
            s"('${ParquetMetadataStoreConf.LEGACY_PARQUET_MD_LOCATION_KEY}')")
        // refresh the metadata
        session.catalog.refreshTable(tableIdentifier)
      case _ =>
    }
  }

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
  override def getRequiredObjects(query: Any, filter: Option[Any]): Future[Set[String]] = Future {
    val q = query.asInstanceOf[Column]
    // using log debug to not expose query to log
    logDebug(s"In search ${q} ...")
    // use repartition to utilize the cluster for filtering
    var df = getRectifiedMetadataDf().select("obj_name").where(q)
    filter match {
      case Some(f) if isPartitionDataAvailable() =>
        val filterExpr = f.asInstanceOf[Expression]
        val mapping: Map[String, String] = filterExpr.references.map(ref =>
          ref.name->getPartitionColName(ref.name)).toMap
        val transformedPartitionFilter = ParquetUtils.replaceReferences(filterExpr, mapping)
        logInfo(s"Original partition filter ${filterExpr.sql}")
        logInfo(s"Transformed partition filter ${transformedPartitionFilter.sql}")
        df = df.where(transformedPartitionFilter.sql)
      case _ =>
    }
    if (DEDUP_ON_FILTER) {
      logInfo("dropping index duplicates")
      df = df.dropDuplicates("obj_name")
    }
    df.collect().map(_.getString(0)).toSet
  }

  /**
    * Returns the metadata df after upgrading it to the current
    * version:
    * 1. the indexes themselves are upgraded in full as if they
    *   were created using the current software version.
    * 2. the KV store (i.e., spark schema metadata) and the partition
    *     keys are not touched (in particular, partition keys are not
    *     added even if the metadata is of an old version that doesn't contain them)
    *
    * @return
    */
  private def getRectifiedMetadataDf(): DataFrame = {
    val baseDf = getMetaDataDFRaw(false)

    val indexes = getIndexes(Registration.getCurrentIndexFactories())
    // note here we are not passing any partition schema since
    // we only add these during an upgrade (i.e., REFRESH).
    ParquetUtils.getTransformedDataFrame(baseDf, indexes, None, false)
  }

  /**
    * Tries to read the metadata as parquet dataframe and throws a metadata corrupted
    * exception if fails. no conversion is done to the column names
    *
    * @param checkVersion whether or not to verify the md is not deprecated.
    *                     if set to true and the md is deprecated, a
    *                     [[ParquetMetaDataStoreException]] is thrown
    * @return
    * @throws ParquetMetaDataStoreException if fails to read the metadata or md is deprecated
    */
  private def getMetaDataDFRaw(checkVersion: Boolean = true): DataFrame = {
    val df = ParquetUtils.mdFileToDF(session, getMDPath.path.toString)
    val versionStatus = ParquetUtils.getMdVersionStatusFromDf(df)
    if (checkVersion &&
      versionStatus != MetadataVersionStatus.CURRENT &&
      versionStatus != MetadataVersionStatus.DEPRECATED_SUPPORTED) {
      val msg = versionStatus match {
        case MetadataVersionStatus.TOO_NEW =>
          "Metadata Version is newer than the" +
            " version supported by this release. please use an adequate jar"
        case MetadataVersionStatus.DEPRECATED_UNSUPPORTED =>
          "Metadata is deprecated and unsupported, please re-index the dataset"
      }
      logWarning(msg)
      throw ParquetMetaDataStoreException(msg)
    }
    df
  }

  /**
    * returns the version status of the metadata.
    * we do not have a strict requirement for the metadatastore to use metadata
    * from a version different than its current version for filtering / refresh
    * but we do expect it to be able to tell the version status,
    * and whether or not it can be upgraded to comply with the current version.
    *
    * @return
    */
  override def getMdVersionStatus(): MetadataVersionStatus = {
    val df = getMetaDataDFRaw(false)
    ParquetUtils.getMdVersionStatusFromDf(df)
  }

  /**
    * returns whether or not the metadata can be upgraded to
    * comply with the current version
    *
    * @return true if the metadata can be upgraded, false otherwise
    */
  override def isMetadataUpgradePossible(): Boolean = {
    val df = getMetaDataDFRaw(false)
    ParquetUtils.isMetadataUpgradePossible(df)
  }

  /**
    * Upgrades the metadata to comply with the current version
    *
    * @param indexes - the indexes stored in the metadataStore.
    */
  override def upgradeMetadata(indexes: Seq[Index], fileIndex: FileIndex): Unit = {
    val baseDf = getMetaDataDFRaw()
    val currVersion = ParquetMetadataStoreConf.PARQUET_MD_STORAGE_VERSION
    val diskVersion = getVersion(baseDf)
    val partSchema = fileIndex.partitionSchema match {
      case x if x.nonEmpty => Some(x)
      case _ => None
    }
    if (diskVersion != currVersion) {
      val dfWithPartitionData = ParquetUtils.getMetadataWithPartitionCols(baseDf, fileIndex)
      val newDf = ParquetUtils.getTransformedDataFrame(dfWithPartitionData,
        indexes, partSchema, true)
      replaceMetaData(newDf)
    }
    // if the index was built on a hive table add the relevant parameter to the table properties.
    // this is also done in finalization phase - but if there are no new files
    // to index then it won't be called and we might have the old params still set
    getMDPath.`type` match {
      case ParquetMetadataStoreConf.PARQUET_MD_LOCATION_HIVE_TABLE_NAME =>
        logInfo("Set the metadata location in the table parameters")
        val mdPath = getMDPath.path.toString
        session.sql(
          s"ALTER TABLE ${tableIdentifier} SET TBLPROPERTIES " +
            s"('${ParquetMetadataStoreConf.PARQUET_MD_LOCATION_KEY}'='${mdPath}')")
        session.sql(
          s"ALTER TABLE ${tableIdentifier} UNSET TBLPROPERTIES IF EXISTS " +
            s"('${ParquetMetadataStoreConf.LEGACY_PARQUET_MD_LOCATION_KEY}')")
        // refresh the table metadata
        session.catalog.refreshTable(tableIdentifier)
      case _ =>
    }
  }

  /**
    * @return the metadata path of the current instance
    */
  def getMDPath(): ParquetMetadataPath = {
    MD_PATH match {
      case Some(parquetMetadataPath) => parquetMetadataPath
      case _ =>
        throw ParquetMetaDataStoreException("Unable to resolve metadata location")
    }
  }

  /**
    * returns the sequence of indexes that exist in the metadatastore for the tableIdentifier
    */
  override def getIndexes(): Seq[Index] = indexesCache match {
    case Some(indexes) => indexes
    case _ =>
      val indexes = getIndexes(Registration.getCurrentIndexFactories())
      indexesCache = Some(indexes)
      indexes
  }

  /**
    * Returns index statistics
    */
  override def getIndexStatus(): IndexStatusResult = {
    refresh()
    val indexes = getIndexes()
    val numberOfIndexedObjects = getNumberOfIndexedObjects()

    val metadataLocation = Utils.getPathDisplayName(getMDPath.path.toString)
    val versionStatus = ParquetUtils.getMdVersionStatus(
      ParquetUtils.getVersion(getMetaDataDFRaw().schema))

    IndexStatusResult(indexes, numberOfIndexedObjects,
      Map("Metadata location" -> metadataLocation), versionStatus)
  }


  private def isPartitionDataAvailable(dfOpt: Option[DataFrame] = None): Boolean = {
    getVersion(dfOpt.getOrElse(getMetaDataDFRaw())) >=
      ParquetMetadataStoreConf.PARQUET_MINIMUM_PARTITION_DATA_VERSION
  }

  /**
    * Refreshes the [[MetadataHandle]] by re-syncing with the metadatastore
    * (implementation specific)
    */
  override def refresh(): Unit = {
    indexesCache = Some(getIndexes(Registration.getCurrentIndexFactories()))
  }

  /**
    * Returns whether or not this [[MetadataHandle]] supports encryption
    *
    * @return true if the [[MetadataHandle]] supports encryption
    */
  override def isEncryptionSupported(): Boolean = true
}
